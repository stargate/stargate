/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.cassandra.datastore;

import static org.apache.cassandra.concurrent.SharedExecutorPool.SHARED;

import com.google.auto.factory.AutoFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import io.stargate.db.Result;
import io.stargate.db.cassandra.impl.CassandraPersistence;
import io.stargate.db.cassandra.impl.Conversion;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ExecutionInfo;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.common.util.ColumnUtils;
import io.stargate.db.datastore.query.Parameter;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.NoSpamLogger;
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

  public static final LocalAwareExecutorService EXECUTOR =
      SHARED.newExecutor(
          DatabaseDescriptor.getNativeTransportMaxThreads(),
          DatabaseDescriptor::setNativeTransportMaxThreads,
          Integer.MAX_VALUE,
          "transport",
          "Native-Transport-Requests");

  // The (?:) construct is just like (), except the former is a non-capturing group
  public static Pattern WRAPPER_CLAUSE_PATTERN = Pattern.compile("^(?:in|key|value)\\((.*)\\)$");

  private final QueryState queryState;
  private final QueryOptions queryOptions;
  private final CassandraPersistence persistence;

  @Inject
  public InternalDataStore(
      CassandraPersistence persistence, QueryState queryState, QueryOptions queryOptions) {
    this.persistence = persistence;
    this.queryState = queryState;
    this.queryOptions = queryOptions;
  }

  public InternalDataStore(CassandraPersistence persistence) {
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

    return new Executor(this, ipsList, vals, Optional.empty())
        .execute(executionTimer)
        .whenComplete(
            (r, t) ->
                LOG.trace(
                    "BEGIN BATCH [... {} statements ...]; APPLY BATCH; took {}ms",
                    statements.size(),
                    executionTimer.stop().elapsed(TimeUnit.MILLISECONDS)));
  }

  private CompletableFuture<Pair<QueryHandler.Prepared, ResultMessage.Prepared>> prepare(
      String cql, QueryState queryState) {
    CompletableFuture<Pair<QueryHandler.Prepared, ResultMessage.Prepared>> future =
        new CompletableFuture<>();

    EXECUTOR.submit(
        () -> {
          try {
            ResultMessage.Prepared prepared =
                QueryProcessor.instance.prepare(cql, queryState.getClientState());
            future.complete(
                Pair.with(QueryProcessor.instance.getPrepared(prepared.statementId), prepared));
          } catch (Throwable t) {
            Conversion.handleException(future, t);
          }
        });

    return future;
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
    private PagingState pagingState;

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
      this.queryOptions = createQueryOptions(null);
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
      this.boundValues = null;
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
          queryState.getClientState(), queryState.getTimestamp(), queryState.getNowInSeconds());
    }

    CompletableFuture<ResultSet> execute(Stopwatch executionTimer) {
      return query().thenApply(resultMessage -> toResultSet(resultMessage, executionTimer));
    }

    CompletableFuture<Result> executeResult(Stopwatch executionTimer) {
      return query()
          .thenApply(
              resultMessage ->
                  Conversion.toResult(resultMessage, queryOptions.getProtocolVersion()));
    }

    CompletableFuture<ResultMessage> query() {
      if (null != prepared) {
        return queryPrepared();
      } else if (null != batchStatements) {
        return queryBatch();
      }
      return queryUnprepared();
    }

    private CompletableFuture<ResultMessage> queryUnprepared() {
      CompletableFuture<ResultMessage> future = new CompletableFuture<>();

      EXECUTOR.submit(
          () -> {
            try {
              CQLStatement statement =
                  QueryProcessor.instance.parse(unpreparedCql, queryState, queryOptions);
              ResultMessage resultMessage =
                  QueryProcessor.instance.process(
                      statement, queryState, queryOptions, null, System.nanoTime());
              if (resultMessage instanceof ResultMessage.Rows) {
                ResultMessage.Rows rows = (ResultMessage.Rows) resultMessage;
                this.pagingState = rows.result.metadata.getPagingState();
              }

              future.complete(resultMessage);
            } catch (Throwable t) {
              Conversion.handleException(future, t);
            }
          });

      return future;
    }

    private CompletableFuture<ResultMessage> queryPrepared() {
      CompletableFuture<ResultMessage> future = new CompletableFuture<>();

      EXECUTOR.submit(
          () -> {
            try {
              ResultMessage resultMessage =
                  QueryProcessor.instance.processPrepared(
                      prepared.statement, queryState, queryOptions, null, System.nanoTime());
              if (resultMessage instanceof ResultMessage.Rows) {
                ResultMessage.Rows rows = (ResultMessage.Rows) resultMessage;
                this.pagingState = rows.result.metadata.getPagingState();
              }

              future.complete(resultMessage);
            } catch (Throwable t) {
              Conversion.handleException(future, t);
            }
          });

      return future;
    }

    private CompletableFuture<ResultMessage> queryBatch() {
      CompletableFuture<ResultMessage> future = new CompletableFuture<>();

      List<ModificationStatement> modificationStatementList =
          new ArrayList<>(batchStatements.size());
      List<Object> statementIds = new ArrayList<>(batchStatements.size());
      List<List<ByteBuffer>> variables = new ArrayList<>(batchStatements.size());

      Iterator<Object[]> batchBoundValuesIterator = batchBoundValues.iterator();

      for (InternalPreparedStatement ips : batchStatements) {
        CachedPreparationInfo cachedPrep;

        try {
          cachedPrep = ips.cache.get();
        } catch (Throwable t) {
          future.completeExceptionally(t);
          return future;
        }

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
          new BatchStatement(
              BatchStatement.Type.LOGGED,
              VariableSpecifications.empty(),
              modificationStatementList,
              Attributes.none());

      BatchQueryOptions batchQueryOptions =
          BatchQueryOptions.withPerStatementVariables(queryOptions, variables, statementIds);

      EXECUTOR.submit(
          () -> {
            try {
              future.complete(
                  QueryProcessor.instance.processBatch(
                      batchStatement,
                      queryState,
                      batchQueryOptions,
                      Collections.emptyMap(),
                      System.nanoTime()));
            } catch (Throwable t) {
              Conversion.handleException(future, t);
            }
          });

      return future;
    }

    Executor withPagingState(PagingState pagingState) {
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
          executionInfoString = prepared.rawCQLStatement;
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

    private boolean isSelectStmt() {
      if (null != prepared) {
        return prepared.statement instanceof SelectStatement;
      }
      if (null != batchStatements) {
        return false;
      }
      return unpreparedCql.regionMatches(true, 0, SELECT, 0, SELECT_LEN);
    }

    private QueryOptions createQueryOptions(PagingState pagingState) {
      int pageSize = -1;
      org.apache.cassandra.db.ConsistencyLevel consistency =
          org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
      boolean skipMetadata = false;
      org.apache.cassandra.db.ConsistencyLevel serialConsistency =
          org.apache.cassandra.db.ConsistencyLevel.SERIAL;
      ProtocolVersion version = ProtocolVersion.CURRENT;
      String keyspace = null;
      List<ByteBuffer> values = boundValues;

      if (consistencyLevel.isPresent()) {
        consistency =
            org.apache.cassandra.db.ConsistencyLevel.fromCode(consistencyLevel.get().code);
      }

      if (null != dataStore.queryOptions) {
        pageSize = dataStore.queryOptions.getPageSize();
        pagingState = dataStore.queryOptions.getPagingState();
        if (!consistencyLevel.isPresent()) {
          consistency = dataStore.queryOptions.getConsistency();
        }
        skipMetadata = dataStore.queryOptions.skipMetadata();
        version = dataStore.queryOptions.getProtocolVersion();
        keyspace = dataStore.queryOptions.getKeyspace();
        serialConsistency = dataStore.queryOptions.getSerialConsistency();

        if (boundValues == null) {
          values = dataStore.queryOptions.getValues();
        }
      }

      return QueryOptions.create(
          consistency,
          values,
          skipMetadata,
          pageSize,
          pagingState,
          serialConsistency,
          version,
          keyspace);
    }

    Schema schema() {
      return dataStore.schema();
    }

    Pair<Integer, PagingState> paging() {
      if (queryOptions.getPageSize() > 0) {
        return Pair.with(queryOptions.getPageSize(), queryOptions.getPagingState());
      }
      return null;
    }

    PagingState getPagingState() {
      return this.pagingState;
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
    private CompletableFuture<CachedPreparationInfo> cache;
    private Schema schema;
    private final Optional<Index> index;

    InternalPreparedStatement(String cql, Schema schema, Optional<Index> index) {
      this.cql = cql;
      this.schema = schema;
      this.index = index;

      this.cache =
          prepare(cql, queryState)
              .thenApply(
                  (p) -> {
                    List<ColumnSpecification> columnSpecifications = p.getValue1().metadata.names;
                    Column[] tableColumns =
                        p.getValue1().metadata.names.stream()
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
                  });
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
        return executePrepared((InternalDataStore) dataStore, cl, parameters);
      }

      return executeUnprepared((InternalDataStore) dataStore, cl, parameters);
    }

    private CompletableFuture<ResultSet> executeUnprepared(
        InternalDataStore dataStore,
        Optional<ConsistencyLevel> consistencyLevel,
        Object[] parameters) {
      Stopwatch executionTimer = Stopwatch.createStarted();

      return new Executor(dataStore, cql, index, consistencyLevel)
          .execute(executionTimer)
          .whenComplete(
              (r, e) ->
                  LOG.trace(
                      "{} took {}ms", cql, executionTimer.stop().elapsed(TimeUnit.MILLISECONDS)));
    }

    private CompletableFuture<ResultSet> executePrepared(
        InternalDataStore dataStore,
        Optional<ConsistencyLevel> consistencyLevel,
        Object[] parameters) {
      convertPlaceholderParameters(parameters);

      Stopwatch executionTimer = Stopwatch.createStarted();

      return cache
          .handle(
              (prepared, t) -> {
                if (t != null) throw new CompletionException(t);
                else return prepared;
              })
          .thenCompose(
              (prepared) -> {
                List<ByteBuffer> boundValues =
                    createBoundValues(
                        prepared.tableColumns, prepared.columnSpecifications, parameters);

                return new Executor(
                        dataStore, prepared.qhPrepared, boundValues, index, consistencyLevel)
                    .execute(executionTimer)
                    .whenComplete(
                        (r, t) -> {
                          LOG.trace(
                              "{} with parameters {} took {}ms",
                              cql,
                              parameters,
                              executionTimer.stop().elapsed(TimeUnit.MILLISECONDS));
                        });
              });
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
