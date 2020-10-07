/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.dse.datastore;

import com.google.auto.factory.AutoFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import io.reactivex.Single;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ExecutionInfo;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.common.util.ColumnUtils;
import io.stargate.db.datastore.query.Parameter;
import io.stargate.db.dse.impl.Conversion;
import io.stargate.db.dse.impl.DsePersistence;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Schema;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.AlterKeyspaceStatement;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.time.ApolloTime;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal {@link DataStore} that runs queries through {@link QueryHandler}/{@link QueryProcessor}.
 */
@AutoFactory
public class InternalDataStore implements DataStore {
  private static final Logger LOG = LoggerFactory.getLogger(InternalDataStore.class);
  private static final NoSpamLogger NO_SPAM_LOG = NoSpamLogger.getLogger(LOG, 1, TimeUnit.MINUTES);

  // The (?:) construct is just like (), except the former is a non-capturing group
  public static Pattern WRAPPER_CLAUSE_PATTERN = Pattern.compile("^(?:in|key|value)\\((.*)\\)$");

  private QueryState queryState;
  private QueryOptions queryOptions;
  private final DsePersistence persistence;

  @Inject
  public InternalDataStore(
      DsePersistence persistence, QueryState queryState, QueryOptions queryOptions) {
    this.persistence = persistence;
    this.queryState = queryState;
    this.queryOptions = queryOptions;
  }

  public InternalDataStore(DsePersistence persistence) {
    this(persistence, QueryState.forInternalCalls(), QueryOptions.DEFAULT);
  }

  @Override
  public CompletableFuture<ResultSet> query(
      String cql, Optional<ConsistencyLevel> consistencyLevel, Object... parameters) {
    return prepare(cql, Optional.empty()).execute(this, consistencyLevel, parameters);
  }

  @Override
  public PreparedStatement prepare(String cql, Optional<Index> index) {
    return new InternalPreparedStatement(cql, schema(), index);
  }

  @Override
  public CompletableFuture<ResultSet> processBatch(
      List<PreparedStatement> statements,
      List<Object[]> vals,
      Optional<ConsistencyLevel> consistencyLevel) {
    List<InternalPreparedStatement> ipsList = new ArrayList<>(statements.size());
    for (PreparedStatement dps : statements) {
      Preconditions.checkArgument(
          dps instanceof InternalPreparedStatement,
          "Unsupported batch statement type: %s",
          dps.getClass());
      ipsList.add((InternalPreparedStatement) dps);
    }

    Stopwatch executionTimer = Stopwatch.createStarted();

    return Conversion.toFuture(
        new Executor(this, ipsList, vals, consistencyLevel)
            .execute(executionTimer)
            .onErrorResumeNext(e -> Single.error(Conversion.handleException(e)))
            .doFinally(
                () ->
                    LOG.trace(
                        "BEGIN BATCH [... {} statements ...]; APPLY BATCH; took {}ms",
                        statements.size(),
                        executionTimer.stop().elapsed(TimeUnit.MILLISECONDS))));
  }

  private Single<Pair<QueryHandler.Prepared, ResultMessage.Prepared>> prepare(
      String cql, QueryState queryState) {
    return QueryProcessor.instance
        .prepare(cql, queryState)
        .map(
            prepared ->
                Pair.with(QueryProcessor.instance.getPrepared(prepared.statementId), prepared));
  }

  private static class CachedPreparationInfo {
    private final QueryHandler.Prepared qhPrepared;
    private final MD5Digest statementId;
    private final Column[] tableColumns;
    private final List<ColumnSpecification> columnSpecifications;

    public CachedPreparationInfo(
        QueryHandler.Prepared qhPrepared,
        MD5Digest statementId,
        Column[] tableColumns,
        List<ColumnSpecification> columnSpecifications) {
      this.qhPrepared = qhPrepared;
      this.statementId = statementId;
      this.tableColumns = tableColumns;
      this.columnSpecifications = columnSpecifications;
    }
  }

  static final class Executor {
    private InternalDataStore dataStore;
    private final QueryHandler.Prepared prepared;
    private final QueryState queryState;
    private final List<ByteBuffer> boundValues;
    private final String unpreparedCql;
    private QueryOptions queryOptions;
    private Optional<Index> index;
    private final Optional<ConsistencyLevel> consistencyLevel;
    private final List<InternalPreparedStatement> batchStatements;
    private final List<Object[]> batchBoundValues;

    private static final String ALTER = "ALTER";
    private static final int ALTER_LEN = ALTER.length();
    private static final String CREATE = "CREATE";
    private static final int CREATE_LEN = CREATE.length();
    private static final String DROP = "DROP";
    private static final int DROP_LEN = DROP.length();
    private static final String SELECT = "SELECT";
    private static final int SELECT_LEN = SELECT.length();
    private static final String TRUNCATE = "TRUNCATE";
    private static final int TRUNCATE_LEN = TRUNCATE.length();

    Executor(
        InternalDataStore dataStore,
        QueryHandler.Prepared prepared,
        List<ByteBuffer> boundValues,
        Optional<Index> index,
        Optional<ConsistencyLevel> consistencyLevel) {
      this.consistencyLevel = consistencyLevel;
      this.dataStore = dataStore;
      this.prepared = prepared;
      this.unpreparedCql = null;
      this.batchStatements = null;
      this.batchBoundValues = null;
      this.queryState = cloneQueryState(dataStore.queryState);
      this.boundValues = boundValues;
      QueryOptions.PagingOptions pagingOptions =
          dataStore.queryOptions != null ? dataStore.queryOptions.getPagingOptions() : null;
      this.queryOptions = createQueryOptions(pagingOptions != null ? pagingOptions.state() : null);
      this.index = index;
    }

    Executor(
        InternalDataStore dataStore,
        String unpreparedCql,
        Optional<Index> index,
        Optional<ConsistencyLevel> consistencyLevel) {
      this.consistencyLevel = consistencyLevel;
      this.dataStore = dataStore;
      this.unpreparedCql = unpreparedCql;
      this.prepared = null;
      this.batchStatements = null;
      this.batchBoundValues = null;
      this.queryState = cloneQueryState(dataStore.queryState);
      this.boundValues = ImmutableList.of();
      this.queryOptions = createQueryOptions(null);
      this.index = index;
    }

    Executor(
        InternalDataStore dataStore,
        List<InternalPreparedStatement> batchStatements,
        List<Object[]> batchBoundValues,
        Optional<ConsistencyLevel> consistencyLevel) {
      this.consistencyLevel = consistencyLevel;
      this.dataStore = dataStore;
      this.unpreparedCql = null;
      this.prepared = null;
      this.batchStatements = batchStatements;
      this.batchBoundValues = batchBoundValues;
      this.queryState = cloneQueryState(dataStore.queryState);
      this.boundValues = ImmutableList.of();
      this.queryOptions = createQueryOptions(null);
      this.index = Optional.empty();
    }

    private QueryState cloneQueryState(QueryState queryState) {
      return new QueryState(
          queryState.getClientState(),
          queryState.getStreamId(),
          queryState.getUserRolesAndPermissions().cloneWithoutAdditionalPermissions());
    }

    Single<ResultSet> execute(Stopwatch executionTimer) {
      return query().map(resultMessage -> toResultSet(resultMessage, executionTimer));
    }

    Single<ResultMessage> query() {
      if (null != prepared) {
        return queryPrepared();
      } else if (null != batchStatements) {
        return queryBatch();
      }
      return queryUnprepared();
    }

    private Single<ResultMessage> queryUnprepared() {
      return QueryProcessor.instance
          .process(unpreparedCql, queryState, queryOptions, ApolloTime.approximateNanoTime())
          .subscribeOn(TPC.bestTPCScheduler());
    }

    private Single<ResultMessage> queryPrepared() {
      return QueryProcessor.instance
          .processStatement(
              prepared.statement, queryState, queryOptions, ApolloTime.approximateNanoTime())
          .subscribeOn(TPC.bestTPCScheduler());
    }

    private Single<ResultMessage> queryBatch() {
      List<ModificationStatement> modificationStatementList =
          new ArrayList<>(batchStatements.size());
      List<Object> statementIds = new ArrayList<>(batchStatements.size());
      List<List<ByteBuffer>> variables = new ArrayList<>(batchStatements.size());

      Iterator<Object[]> batchBoundValuesIterator = batchBoundValues.iterator();

      for (InternalPreparedStatement ips : batchStatements) {
        CachedPreparationInfo cachedPrep = ips.cache.blockingGet();
        CQLStatement cqlStatement = cachedPrep.qhPrepared.statement;
        if (!(cqlStatement instanceof ModificationStatement)) {
          throw new IllegalArgumentException("Statement cannot be batched: " + cqlStatement);
        }
        modificationStatementList.add((ModificationStatement) cqlStatement);
        statementIds.add(cachedPrep.statementId);

        Object vals[] = batchBoundValuesIterator.next();
        InternalDataStore.convertPlaceholderParameters(vals);
        List<ByteBuffer> serializedParamValues =
            ips.createBoundValues(cachedPrep.tableColumns, cachedPrep.columnSpecifications, vals);
        variables.add(serializedParamValues);
      }

      BatchStatement batchStatement =
          BatchStatement.of(BatchStatement.Type.LOGGED, modificationStatementList);

      BatchQueryOptions batchQueryOptions =
          BatchQueryOptions.withPerStatementVariables(queryOptions, variables, statementIds);

      return QueryProcessor.instance
          .processBatch(
              batchStatement,
              queryState,
              batchQueryOptions,
              Collections.emptyMap(),
              ApolloTime.approximateNanoTime())
          .subscribeOn(TPC.bestTPCScheduler());
    }

    Executor withPagingState(ByteBuffer pagingState) {
      queryOptions = createQueryOptions(pagingState);
      return this;
    }

    private ResultSet toResultSet(ResultMessage resultMessage, Stopwatch executionTimer) {
      boolean schemaAltering = false;

      if (resultMessage instanceof ResultMessage.SchemaChange) {
        schemaAltering = true;
        dataStore.waitForSchemaAgreement();
      } else if (resultMessage instanceof ResultMessage.Rows) {
        final String executionInfoString;
        if (null != unpreparedCql) {
          executionInfoString = unpreparedCql;
        } else if (null != prepared) {
          executionInfoString = prepared.statement.getQueryString();
        } else {
          executionInfoString =
              String.format(
                  "BEGIN BATCH [... %s statements ...]; APPLY BATCH;", batchStatements.size());
        }
        return new InternalResultSet(
            this,
            (ResultMessage.Rows) resultMessage,
            schemaAltering,
            ExecutionInfo.create(
                executionInfoString, executionTimer.elapsed(TimeUnit.NANOSECONDS), index));
      }

      return ResultSet.empty(schemaAltering);
    }

    private boolean isSchemaAltering() {
      if (null != prepared) {
        return prepared.statement instanceof AlterKeyspaceStatement;
      }
      if (null != batchStatements) {
        return false;
      }
      return unpreparedCql.regionMatches(true, 0, CREATE, 0, CREATE_LEN)
          || unpreparedCql.regionMatches(true, 0, TRUNCATE, 0, TRUNCATE_LEN)
          || unpreparedCql.regionMatches(true, 0, DROP, 0, DROP_LEN)
          || unpreparedCql.regionMatches(true, 0, ALTER, 0, ALTER_LEN);
    }

    private boolean isSelectStmt() {
      if (null != prepared) {
        return prepared.statement instanceof SelectStatement;
      }
      if (null != batchStatements) {
        return false;
      }
      return unpreparedCql.regionMatches(true, 0, SELECT, 0, SELECT_LEN);
    }

    private QueryOptions createQueryOptions(ByteBuffer pagingState) {

      QueryOptions.PagingOptions pagingOptions = null;
      org.apache.cassandra.db.ConsistencyLevel consistency =
          org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
      boolean skipMetadata = false;
      org.apache.cassandra.db.ConsistencyLevel serialConsistency =
          org.apache.cassandra.db.ConsistencyLevel.SERIAL;
      ProtocolVersion version = ProtocolVersion.CURRENT;
      String keyspace = null;

      if (consistencyLevel.isPresent()) {
        consistency =
            org.apache.cassandra.db.ConsistencyLevel.fromCode(consistencyLevel.get().code);
      }

      if (null != dataStore.queryOptions) {
        pagingOptions = dataStore.queryOptions.getPagingOptions();
        if (!consistencyLevel.isPresent()) {
          consistency = dataStore.queryOptions.getConsistency();
        }
        skipMetadata = dataStore.queryOptions.skipMetadata();
        version = dataStore.queryOptions.getProtocolVersion();
        keyspace = dataStore.queryOptions.getKeyspace();
        serialConsistency = dataStore.queryOptions.getSerialConsistency(queryState);
      }

      final int pageSize =
          pagingOptions == null
                  || dataStore.queryOptions.getPagingOptions().pageSize().inRows() <= 0
              // note: most drivers now send a default page size of 5000, but the protocol spec
              // doesn't
              // list it as mandatory so this check is still necessary
              ? DEFAULT_ROWS_PER_PAGE
              : pagingOptions.pageSize().inRows();

      if (isSelectStmt()) {
        pagingOptions =
            new QueryOptions.PagingOptions(
                new PageSize(pageSize, PageSize.PageUnit.ROWS),
                QueryOptions.PagingOptions.Mechanism.SINGLE,
                pagingState);
      }

      return QueryOptions.create(
          consistency,
          boundValues,
          skipMetadata,
          pagingOptions,
          serialConsistency,
          version,
          keyspace);
    }

    Schema schema() {
      return dataStore.schema();
    }

    QueryOptions.PagingOptions paging() {
      return queryOptions.getPagingOptions();
    }
  }

  @Override
  public Schema schema() {
    return persistence.schema();
  }

  @Override
  public boolean isInSchemaAgreement() {
    return persistence.isInSchemaAgreement();
  }

  public QueryState queryState() {
    return queryState;
  }

  private class InternalPreparedStatement implements PreparedStatement {
    private final String cql;
    private Single<CachedPreparationInfo> cache;
    private Schema schema;
    private final Optional<Index> index;

    InternalPreparedStatement(String cql, Schema schema, Optional<Index> index) {
      this.cql = cql;
      this.schema = schema;
      this.index = index;

      this.cache =
          prepare(cql, queryState)
              .map(
                  p -> {
                    List<ColumnSpecification> columnSpecifications =
                        p.getValue1().metadata.names.stream()
                            .map(
                                c -> {
                                  String keyspace =
                                      c.ksName.equals("system_views")
                                          ? SchemaConstants.SYSTEM_KEYSPACE_NAME
                                          : c.ksName;
                                  String table = c.cfName;
                                  if (table.equals("local_node")) table = SystemKeyspace.LOCAL;
                                  else if (table.equals("peer_nodes")) table = SystemKeyspace.PEERS;

                                  return new ColumnSpecification(keyspace, table, c.name, c.type);
                                })
                            .collect(Collectors.toList());

                    Column[] tableColumns =
                        columnSpecifications.stream()
                            .map(
                                c ->
                                    schema
                                        .keyspace(c.ksName)
                                        .getColumnFromTableOrIndex(
                                            c.cfName, extractColumnName(c.name.toString())))
                            .toArray(Column[]::new);
                    MD5Digest statementId = p.getValue1().statementId;
                    return new CachedPreparationInfo(
                        p.getValue0(), statementId, tableColumns, columnSpecifications);
                  })
              .cache();
    }

    /**
     * Remove an in(...), key(...), or value(...) text wrapper around a column name.
     *
     * <p>Related issues: DSP-15358, DSP-15923, DSP-18689
     *
     * <p>The following note is from DSP-15358: We're extracting the column name from the prepared
     * statement's metadata. However, when a CQL contains the IN clause, then C* will rename a
     * column X to 'in(X)' in its metadata and so we need to remove the 'in()' part from the column
     * name. Apparently there's no better fix atm for this. Indexes on a set/list/map will wrap the
     * column X in a 'value(X)'.
     *
     * @param name The column name
     * @return The column name without being wrapped by 'in()', 'key()', or 'value()'.
     */
    private String extractColumnName(String name) {
      Matcher m = WRAPPER_CLAUSE_PATTERN.matcher(name);
      if (m.matches()) {
        return m.group(1);
      }
      return name;
    }

    @Override
    public CompletableFuture<ResultSet> execute(
        DataStore dataStore, Optional<ConsistencyLevel> cl, Object... parameters) {
      if (null != cache) {
        return Conversion.toFuture(executePrepared((InternalDataStore) dataStore, cl, parameters));
      }

      return Conversion.toFuture(executeUnprepared((InternalDataStore) dataStore, cl, parameters));
    }

    private Single<ResultSet> executeUnprepared(
        InternalDataStore dataStore,
        Optional<ConsistencyLevel> consistencyLevel,
        Object[] parameters) {
      Stopwatch executionTimer = Stopwatch.createStarted();

      return new Executor(dataStore, cql, index, consistencyLevel)
          .execute(executionTimer)
          .onErrorResumeNext(e -> Single.error(Conversion.handleException(e)))
          .doFinally(
              () ->
                  LOG.trace(
                      "{} took {}ms", cql, executionTimer.stop().elapsed(TimeUnit.MILLISECONDS)));
    }

    private Single<ResultSet> executePrepared(
        InternalDataStore dataStore,
        Optional<ConsistencyLevel> consistencyLevel,
        Object[] parameters) {
      Stopwatch executionTimer = Stopwatch.createStarted();

      convertPlaceholderParameters(parameters);

      return Single.defer(() -> cache)
          .flatMap(
              prepared -> {
                List<ByteBuffer> boundValues =
                    createBoundValues(
                        prepared.tableColumns, prepared.columnSpecifications, parameters);
                return new Executor(
                        dataStore, prepared.qhPrepared, boundValues, index, consistencyLevel)
                    .execute(executionTimer)
                    .onErrorResumeNext(e -> Single.error(Conversion.handleException(e)));
              })
          .doFinally(
              () ->
                  LOG.trace(
                      "{} with parameters {} took {}ms",
                      cql,
                      parameters,
                      executionTimer.stop().elapsed(TimeUnit.MILLISECONDS)));
    }

    // copied from internal C* code and slightly adjusted
    private List<ByteBuffer> createBoundValues(
        Column[] columns, List<ColumnSpecification> columnSpecifications, Object[] values) {
      if (columns.length == 0) {
        return Collections.emptyList();
      }
      Preconditions.checkArgument(
          columns.length == values.length,
          "Unexpected number of parameters expected %s but got %s",
          columns.length,
          values.length);
      List<ByteBuffer> boundValues = new ArrayList<>(values.length);
      for (int i = 0; i < values.length; i++) {
        Column column = columns[i];
        ColumnSpecification spec = columnSpecifications.get(i);
        AbstractTable table = schema.keyspace(spec.ksName).tableOrMaterializedView(spec.cfName);
        Preconditions.checkNotNull(table, "Table '%s' was not found", spec.cfName);
        Object value = values[i];
        if (value == null) {
          validateParameter(table, spec.name.toString(), value);
          boundValues.add(null);
        } else if (value == ByteBufferUtil.UNSET_BYTE_BUFFER) {
          validateParameter(table, spec.name.toString(), Parameter.UNSET);
          boundValues.add((ByteBuffer) value);
        } else if (colNameStartsWithIgnoreCase(spec, IN, IN_LEN)
            || colNameStartsWithIgnoreCase(spec, VALUE, VALUE_LEN)
            || colNameStartsWithIgnoreCase(spec, KEY, KEY_LEN)) {
          // the type from the prepared stmt metadata will correctly decompose
          // if a collection type is passed to the IN clause.
          // We can't use this approach here only, because we're using LocalDate from the Driver
          // and so we need to also do the stuff in the else branch.
          // the type will also be correctly decomposed if we're looking at a Column that's
          // a CQL collection.
          // in(x) -> when using the WITHIN predicate on column x
          // value(x) -> when x is a collection
          // key(x) -> when x is the key of a map
          value = DataStoreUtil.maybeExtractParameterIfMapPair(value);
          value = validateParameter(table, spec.name.toString(), value);
          if (column.type().isParameterized()) {
            // if we're querying a CQL collection, then we need to convert the user value to the
            // internal
            // value using the correct type information from the underlying column type
            int parameterIdx =
                column.type().rawType() == Column.Type.Map
                        && colNameStartsWithIgnoreCase(spec, VALUE, VALUE_LEN)
                    ? 1
                    : 0;
            value =
                ColumnUtils.toInternalValue(column.type().parameters().get(parameterIdx), value);
          }
          ByteBuffer val = ((AbstractType) spec.type).decompose(value);
          boundValues.add(val);
        } else {
          value = validateParameter(table, spec.name.toString(), value);
          value = ColumnUtils.toInternalValue(column.type(), value);
          ByteBuffer val = ColumnUtils.toInternalType(column.type()).decompose(value);
          boundValues.add(val);
        }
      }

      return boundValues;
    }

    private boolean colNameStartsWithIgnoreCase(
        ColumnSpecification spec, String prefix, int prefixLength) {
      return spec.name.toString().regionMatches(true, 0, prefix, 0, prefixLength);
    }

    @Override
    public String toString() {
      return cql;
    }
  }

  public static void convertPlaceholderParameters(Object parameters[]) {
    for (int count = 0; count < parameters.length; count++) {
      Object parameter = parameters[count];
      if (Parameter.UNSET.equals(parameter)) {
        parameters[count] = ByteBufferUtil.UNSET_BYTE_BUFFER;
      } else if (Parameter.NULL.equals(parameter)) {
        parameters[count] = null;
      }
    }
  }
}
