package io.stargate.db.dse.impl;

import io.reactivex.Single;
import io.stargate.db.dse.impl.interceptors.QueryInterceptor;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

public class StargateQueryHandler implements QueryHandler {

  private final List<QueryInterceptor> interceptors = new CopyOnWriteArrayList<>();

  void register(QueryInterceptor interceptor) {
    this.interceptors.add(interceptor);
  }

  @Override
  public Single<ResultMessage> process(
      String query,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {

    QueryState state = queryState.cloneWithKeyspaceIfSet(options.getKeyspace());
    CQLStatement statement;
    try {
      statement = QueryProcessor.getStatement(query, state);
      options.prepare(statement.getBindVariables());
    } catch (Exception e) {
      return QueryProcessor.auditLogger.logFailedQuery(query, state, e).andThen(Single.error(e));
    }

    if (!queryState.isSystem()) QueryProcessor.metrics.regularStatementsExecuted.inc();

    return processStatement(statement, state, options, customPayload, queryStartNanoTime);
  }

  @Override
  public Single<ResultMessage.Prepared> prepare(
      String query, QueryState queryState, Map<String, ByteBuffer> customPayload) {
    return QueryProcessor.instance.prepare(query, queryState, customPayload);
  }

  @Override
  public Prepared getPrepared(MD5Digest id) {
    return QueryProcessor.instance.getPrepared(id);
  }

  @Override
  public Single<ResultMessage> processStatement(
      CQLStatement statement,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {

    for (QueryInterceptor interceptor : interceptors) {
      Single<ResultMessage> result =
          interceptor.interceptQuery(
              statement, queryState, options, customPayload, queryStartNanoTime);
      if (result != null) {
        return result;
      }
    }

    return QueryProcessor.instance.processStatement(
        statement, queryState, options, customPayload, queryStartNanoTime);
  }

  @Override
  public Single<ResultMessage> processBatch(
      BatchStatement batchStatement,
      QueryState queryState,
      BatchQueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {
    return QueryProcessor.instance.processBatch(
        batchStatement, queryState, options, customPayload, queryStartNanoTime);
  }
}
