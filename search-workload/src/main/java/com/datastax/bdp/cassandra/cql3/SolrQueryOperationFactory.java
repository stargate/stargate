/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.db.audit.AuditableEvent;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex;
import com.datastax.bdp.search.solr.cql.*;
import com.datastax.bdp.server.DseDaemon;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.AggregateFcts;
import org.apache.cassandra.cql3.restrictions.SearchRawStatement;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.statements.QualifiedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DateRangeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Solr-aware operation factory. It would be nice to have SolrStandardOperation extends Operation
 * and override execute, but the Operation classes are all non-static so they can use the plugins
 * and such inside DseQueryHandler. This approach does have the advantage of removing a lot of
 * constructor boilerplate, though.
 */
public class SolrQueryOperationFactory extends DseStandardQueryOperationFactory {
  private QueryHandler queryHandler;

  public SolrQueryOperationFactory() {}

  public SolrQueryOperationFactory(QueryHandler stargateQueryHandler) {
    this.queryHandler = stargateQueryHandler;
  }

  private enum OrderingType {
    SOLR_ONLY,
    CASSANDRA_COMPATIBLE,
    SOLR_INCOMPATIBLE,
    NONE
  }

  private final CqlSolrQueryExecutor solrQueryExecutor = new CqlSolrQueryExecutor();

  // See DSP-13651 for details around how we reached this default:
  public static final int MAX_QUERY_THREADS =
      Integer.getInteger("dse.search.query.threads", FBUtilities.getAvailableProcessors() * 2);

  private static final TimeUnit LATENCY_UNIT = TimeUnit.MICROSECONDS;

  private final Scheduler QUERY_SCHEDULER =
      Schedulers.from(
          new ThreadPoolExecutor(
              MAX_QUERY_THREADS,
              MAX_QUERY_THREADS,
              30L,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<>(),
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setThreadFactory(new UninterruptibleThreadFactory())
                  .setNameFormat("Solr CQL query thread-%d")
                  .build()));

  // Solr threads must not be interrupted so this ThreadFactory produces
  // Thread instances that override and throw away any interrupts
  private static class UninterruptibleThreadFactory implements ThreadFactory {
    private static final Logger logger =
        LoggerFactory.getLogger(UninterruptibleThreadFactory.class);
    private static final NoSpamLogger noSpamLogger =
        NoSpamLogger.getLogger(logger, 30, TimeUnit.SECONDS);

    @Override
    public Thread newThread(@NotNull Runnable r) {
      return new Thread(r) {
        @Override
        public void interrupt() {
          noSpamLogger.debug(
              "A search query received an interrupt on " + this.getName() + ". This was ignored.",
              new Throwable());
        }
      };
    }
  }

  public Single<ResultMessage> executeSolrStatement(
      String query,
      CQLStatement statement,
      SolrQueryType solrStmntType,
      QueryOptions options,
      QueryState state,
      Map<String, ByteBuffer> payload) {
    assert TPCUtils.isTPCThread() : "Solr queries should arrive only on TPC threads.";

    Stopwatch stopwatch = Stopwatch.createStarted();

    SelectStatement select = (SelectStatement) statement;
    String indexName = select.keyspace() + "." + select.table.name;
    // TODO-SEARCH
    // QueryMetrics metrics = QueryMetrics.getInstance(indexName);
    // metrics.incrementEnqueuedRequests();

    // Apply backpressure so that queries do not accumulate to the point where we put pressure on
    // the heap.
    // (see https://datastax.jira.com/wiki/spaces/EN/pages/540475409/TPC+Backpressure)
    int coreId = TPCUtils.getCoreId();
    TPCRunnable backpressureTask =
        new TPCRunnable(() -> {}, ExecutorLocals.create(), TPCTaskType.READ_LOCAL, coreId);
    TPC.eventLoopGroup()
        .getBackpressureController()
        .onPending(TPCTaskType.READ_LOCAL, true, coreId);
    backpressureTask.setPending();
    return Single.fromCallable(
            () -> {
              long timeInQueue = stopwatch.elapsed(LATENCY_UNIT);
              // TODO-SEARCH
              // metrics.recordLatency(QueryMetrics.QueryPhase.ENQUEUE, "", timeInQueue,
              // LATENCY_UNIT);
              // metrics.decrementEnqueuedRequests();

              try {
                statement.authorize(state);

                // Create the appropriate Solr select statement decorator depending on whether
                // that statement embeds a query in "solr_query" or uses the native CQL syntax:
                // TODO(versaurabh)
                //                if (payload != null &&
                // payload.containsKey("stargate.auth.subject.token")) {
                //                  this.stargateQueryHandler.authorizeByToken(payload, statement);
                //                }
                SolrSelectStatement stmnt =
                    solrStmntType.equals(SolrQueryType.EMBEDDED)
                        ? new EmbeddedSolrQueryStatement(statement, options, state)
                        : new NativeCQLSolrSelectStatement(statement, options, state, payload);

                return (ResultMessage)
                    solrQueryExecutor.execute(query, stmnt, options, state, payload);
              } finally {
                TPC.eventLoopGroup()
                    .getBackpressureController()
                    .onPending(TPCTaskType.READ_LOCAL, false, coreId);
                backpressureTask.unsetPending();
                backpressureTask.run();
                long totalTime = stopwatch.elapsed(LATENCY_UNIT);
                // TODO-SEARCH
                // metrics.recordLatency(QueryMetrics.QueryPhase.TOTAL, "", totalTime,
                // LATENCY_UNIT);
              }
            })
        .subscribeOn(QUERY_SCHEDULER);
  }

  @Override
  public StandardOperation create(
      String cql,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> payload,
      long queryStartNanoTime,
      boolean auditStatement) {
    return new StandardOperation(cql, state, options, payload, queryStartNanoTime, auditStatement) {
      @Override
      public Single<ResultMessage> execute() {
        SolrQueryType solrStmntType = SolrQueryType.fromStatement(statement, options, state);
        return !solrStmntType.equals(SolrQueryType.NONE)
            ? executeSolrStatement(cql, statement, solrStmntType, options, state, payload)
            // : super.execute();
            : queryHandler.processStatement(
                statement, queryState, options, payload, queryStartNanoTime);
      }

      @Override
      void parse() {
        CQLStatement.Raw onlyParsed =
            StatementUtils.parseMaybeInjectCustomValidation(
                cql, SolrQueryOperationFactory.this, state.getClientState(), payload);

        // Set keyspace for statement that require login
        if (onlyParsed instanceof QualifiedStatement) {
          ((QualifiedStatement) onlyParsed).setKeyspace(state.getClientState());
        }
        Tracing.trace("Preparing statement");

        statement = onlyParsed.prepare(state.getClientState());
      }

      @Override
      List<AuditableEvent> getEventsFromAuditLogger() {
        SolrQueryType solrStmntType = SolrQueryType.fromStatement(statement, options, state);
        return solrStmntType.equals(SolrQueryType.NONE)
            ? super.getEventsFromAuditLogger()
            : Collections.singletonList(
                new AuditableEvent(
                    queryState,
                    CoreAuditableEventType.CQL_SELECT,
                    null,
                    ((SelectStatement) statement).keyspace(),
                    ((SelectStatement) statement).table(),
                    cql,
                    ConsistencyLevel.ONE));
      }
    };
  }

  @Override
  public PreparedOperation createPrepared(
      CQLStatement statement,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> payload,
      long queryStartNanoTime,
      boolean auditStatement) {
    return new PreparedOperation(
        statement, state, options, payload, queryStartNanoTime, auditStatement) {
      @Override
      public Single<ResultMessage> execute() {
        SolrQueryType solrStmntType = SolrQueryType.fromStatement(statement, options, state);
        return !solrStmntType.equals(SolrQueryType.NONE)
            ? executeSolrStatement(cql, statement, solrStmntType, options, state, payload)
            // : super.execute();
            : queryHandler.processStatement(
                statement, queryState, options, payload, queryStartNanoTime);
      }
    };
  }

  @Override
  public CQLStatement.Raw maybeInjectCustomRestrictions(
      CQLStatement.Raw statement, Map<String, ByteBuffer> customPayload) {
    Preconditions.checkArgument(
        statement instanceof SelectStatement.Raw,
        "Statement was not a SELECT " + statement.toString());

    if (isNativeSearchQueryDisabled(customPayload)) {
      return statement;
    }

    SelectStatement.Raw rawSelect = (SelectStatement.Raw) statement;
    if (hasSolrIncompatibleParameter(rawSelect)) {
      return statement;
    }

    TableMetadata tableMetadata =
        SchemaManager.instance.getTableMetadata(rawSelect.keyspace(), rawSelect.name());
    if (tableMetadata == null) {
      return statement;
    }

    IndexMetadata indexMetadata = findSearchIndex(tableMetadata);
    if (indexMetadata == null) {
      return statement;
    }

    // Not supported in CS tables
    if (!tableMetadata.isCQLTable()) {
      return statement;
    }

    // Get the relations (don't even attempt to create a new list if there WHERE clause is empty)
    List<SingleColumnRelation> relations =
        rawSelect.whereClause.relations.isEmpty()
            ? Collections.emptyList()
            : extractSingleColumnRelations(rawSelect);

    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableMetadata);
    Cql3SolrSecondaryIndex index =
        (Cql3SolrSecondaryIndex) cfs.indexManager.getIndex(indexMetadata);
    OrderingType orderingType = getOrderingType(rawSelect, tableMetadata, relations, index);

    if (orderingType == OrderingType.SOLR_INCOMPATIBLE) {
      return statement;
    }

    if ((relations.isEmpty() && orderingType == OrderingType.NONE)
        || hasSolrIncompatibleSelector(rawSelect)) {
      return statement;
    }

    // Inspect relations & operators
    int partitionElementsRestricted = 0;
    int regularColumnsRestricted = 0;
    Map<ColumnMetadata, Operator> restrictedClusteringKeys = new HashMap<>();
    boolean hasSolrOnlyOperator = false;
    boolean hasSolrPreferredType = false;
    for (Object currentObject : relations) {
      SingleColumnRelation relation = (SingleColumnRelation) currentObject;
      ByteBuffer columnName = relation.getEntity().getIdentifier(tableMetadata).bytes;
      ColumnMetadata column = tableMetadata.getColumn(columnName);

      if (column == null) {
        continue;
      }

      // Check for solr_query and other early terminations
      if (columnName.equals(DseDaemon.SOLR_QUERY_KEY_BB) || !index.dependsOn(column)) {
        return statement;
      }

      // There types are backed by a string in Solr and other scenarios so only some operators are
      // possible
      if ((column.type instanceof DecimalType
              || column.type instanceof IntegerType
              || column.type instanceof BooleanType)
          && !(relation.operator().equals(Operator.EQ)
              || relation.operator().isIN()
              || relation.operator().equals(Operator.IS_NOT)
              || relation.operator().equals(Operator.NEQ))) {
        return statement;
      }

      if (tableMetadata.partitionKeyColumns().contains(column)) {
        partitionElementsRestricted++;
      } else if (tableMetadata.clusteringColumns().contains(column)) {
        restrictedClusteringKeys.put(column, relation.operator());
      } else {
        regularColumnsRestricted++;
      }

      // Check if an operator forces Solr execution
      hasSolrOnlyOperator |=
          relation.isLIKE()
              || relation.operator().equals(Operator.IS_NOT)
              || relation.operator().equals(Operator.NEQ);
      hasSolrOnlyOperator |=
          relation.isSlice() && tableMetadata.partitionKeyColumns().contains(column);

      // Some types, like DateRangeType we route to the Solr path when possible
      hasSolrPreferredType |= column.type instanceof DateRangeType;
    }

    boolean isCassandraIncompatible =
        regularColumnsRestricted > 0 // Would fail with ALLOW FILTERING in C*
            || hasSolrOnlyOperator
            || hasSolrPreferredType
            || orderingType == OrderingType.SOLR_ONLY
            || partitionElementsRestricted < tableMetadata.partitionKeyColumns().size()
            || hasSolrOnlyClusteringRestriction(tableMetadata, restrictedClusteringKeys);

    // If all conditions are met then we have an expressed solr query
    if (isCassandraIncompatible) {
      return injectCustomValidation(rawSelect);
    }

    return statement;
  }

  private List<SingleColumnRelation> extractSingleColumnRelations(
      SelectStatement.Raw rawStatement) {
    // TODO: Is there a better way to avoid the pointless array re-size on the first addition?
    List<SingleColumnRelation> relations = new ArrayList<>(2);

    for (Relation current : rawStatement.whereClause.relations) {
      if (current instanceof SingleColumnRelation) {
        relations.add((SingleColumnRelation) current);
      }
    }

    return relations;
  }

  private boolean hasSolrOnlyClusteringRestriction(
      TableMetadata table, Map<ColumnMetadata, Operator> restrictions) {
    boolean hasSolrOnlyClusteringRestriction = false;

    if (!restrictions.isEmpty()) {
      Pair<ColumnMetadata, Operator> lastRestriction = null;
      int columnIndex = 0;
      for (ColumnMetadata current : table.clusteringColumns()) {
        if (restrictions.get(current) == null) {
          // The last CK restriction in the SELECT can only be one of certain operators in C* and CK
          // list must have no gaps
          if (lastRestriction != null
              && columnIndex
                  == restrictions
                      .size() // SELECT has no restrictions on 'later' CKs -> gaps -> only can be
              // ran on Solr
              && (lastRestriction.getRight().isSlice()
                  || lastRestriction.getRight().isIN()
                  || restrictions.get(lastRestriction.getLeft()).equals(Operator.EQ))) {
            hasSolrOnlyClusteringRestriction = false;
            break;
          } else {
            hasSolrOnlyClusteringRestriction = true;
            break;
          }
        } else if (columnIndex == (table.clusteringColumns().size() - 1)) {
          if (restrictions.get(current).isSlice()
              || restrictions.get(current).isIN()
              || restrictions.get(current).equals(Operator.EQ)) {
            hasSolrOnlyClusteringRestriction = false;
            break;
          } else {
            hasSolrOnlyClusteringRestriction = true;
            break;
          }
        }
        lastRestriction = Pair.of(current, restrictions.get(current));
        columnIndex++;
      }
    }

    return hasSolrOnlyClusteringRestriction;
  }

  private boolean hasSolrIncompatibleSelector(SelectStatement.Raw rawSelect) {
    // Don't activate Solr path if anything else than RawIdentifiers, count() or UDT subfield in
    // projection clause
    for (RawSelector current : rawSelect.selectClause) {
      if (!(current.selectable instanceof Selectable.RawIdentifier)
          && !(current.selectable instanceof Selectable.WithFieldSelection.Raw)
          && !(current.selectable instanceof Selectable.WithFunction.Raw
              && ((Selectable.WithFunction.Raw) current.selectable)
                  .functionName.equals(AggregateFcts.countRowsFunction.name()))) {
        return true;
      }
    }
    return false;
  }

  private OrderingType getOrderingType(
      SelectStatement.Raw rawSelect,
      TableMetadata table,
      List<SingleColumnRelation> relations,
      Cql3SolrSecondaryIndex index) {
    /*
     * ORDER BY is only supported for raw C* queries when:
     * 1.) The partition key is restricted by = or IN. However, multi-valued IN restrictions
     *     on the partition key with ORDER BY are not supported if paging is enabled.
     * 2.) The columns specified in ORDER BY must be an ordered subset of the columns of the
     *     clustering key. However, columns restricted by = or a single-valued IN restriction can be skipped.
     *     ex. SELECT * FROM foo WHERE pk = 1 AND clustering2 = 1 ORDER BY clustering1, clustering3 ALLOW FILTERING
     * 3.) All columns must be either reversed or not.
     *     ex. ORDER BY a ASC, b DESC is invalid for CLUSTERING ORDER BY (a ASC, b ASC).
     */

    OrderingType type = OrderingType.CASSANDRA_COMPATIBLE;

    if (rawSelect.parameters.orderings.isEmpty()) {
      type = OrderingType.NONE;
    } else {
      Boolean firstReversed = rawSelect.parameters.orderings.values().iterator().next();

      int i = 0;

      // The orderings are actually a LinkedHashMap
      for (Map.Entry<ColumnMetadata.Raw, Boolean> current :
          rawSelect.parameters.orderings.entrySet()) {
        ColumnMetadata column = current.getKey().prepare(table);

        // 1.) Solr cannot sort on a field that is neither indexed nor has docValues.
        // 2.) CQL "decimal" and "bigint" are string-based in Solr, and would not sort numerically.
        if (column.type instanceof DecimalType
            || column.type instanceof IntegerType
            || !index.dependsOn(column)) {
          return OrderingType.SOLR_INCOMPATIBLE;
        }

        if (!column.isClusteringColumn() || current.getValue() != firstReversed) {
          type = OrderingType.SOLR_ONLY;
          continue;
        }

        // Advance the current clustering position past already restricted elements:
        while (i != column.position()) {
          ColumnMetadata restricted = table.clusteringColumns().get(i);
          SingleColumnRelation equalityRelation = null;

          // Determine whether the clustering key element is restricted by EQ or a single-value IN
          // predicate:
          for (SingleColumnRelation relation : relations) {
            if (relation.getEntity().getIdentifier(table).equals(restricted.name)) {
              if (relation.operator() == Operator.EQ
                  || relation.operator() == Operator.IN && relation.getInValues().size() == 1) {
                equalityRelation = relation;
                break;
              }
            }
          }

          // If we've simply skipped the column in the sort, only Solr might be able to handle it:
          if (equalityRelation == null) {
            type = OrderingType.SOLR_ONLY;
          }

          i++;
        }

        i++;
      }

      // An empty WHERE with an ORDER BY fails in C*. If all ORDER BY cols are indexed, we can run
      // it on Solr.
      if (relations.isEmpty()) {
        type = OrderingType.SOLR_ONLY;
      }
    }

    return type;
  }

  private IndexMetadata findSearchIndex(TableMetadata metadata) {
    IndexMetadata indexMeta = null;
    for (IndexMetadata current : metadata.indexes) {
      if (current.options.get("target").equals(DseDaemon.SOLR_QUERY_KEY)
          && current.options.get("class_name").equals(Cql3SolrSecondaryIndex.class.getName())) {
        indexMeta = current;
        break;
      }
    }
    return indexMeta;
  }

  private boolean hasSolrIncompatibleParameter(SelectStatement.Raw rawSelect) {
    return rawSelect.parameters.allowFiltering
        || rawSelect.parameters.isDistinct
        || rawSelect.parameters.groups.size() != 0
        || rawSelect.perPartitionLimit != null;
  }

  private boolean isNativeSearchQueryDisabled(Map<String, ByteBuffer> customPayload) {
    try {
      if (customPayload != null) {
        ByteBuffer payloadValue = customPayload.get(NativeCQLSolrSelectStatement.DEACTIVATION_KEY);

        if (payloadValue != null && Boolean.parseBoolean(ByteBufferUtil.string(payloadValue))) {
          return true;
        }
      }
    } catch (CharacterCodingException e) {
      throw new InvalidRequestException("Could not decode incoming payload: " + e.getMessage());
    }

    return false;
  }

  private CQLStatement.Raw injectCustomValidation(SelectStatement.Raw statement) {
    SelectStatement.Raw customizedStatement = SearchRawStatement.fromRawStatement(statement);
    customizedStatement.setQueryString(statement.getQueryString());
    customizedStatement.setVariableSpecifications(statement.getVariableSpecifications());
    return customizedStatement;
  }
}
