package io.stargate.db.cassandra.impl;

import io.stargate.db.cassandra.impl.interceptors.QueryInterceptor;
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
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

public class StargateQueryHandler implements QueryHandler {
  private final List<QueryInterceptor> interceptors = new CopyOnWriteArrayList<>();

  void register(QueryInterceptor interceptor) {
    this.interceptors.add(interceptor);
  }

  private ResultMessage maybeIntercept(
      CQLStatement statement,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {
    for (QueryInterceptor interceptor : interceptors) {
      ResultMessage result =
          interceptor.interceptQuery(
              statement, queryState, options, customPayload, queryStartNanoTime);
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  @Override
  public CQLStatement parse(String s, QueryState queryState, QueryOptions queryOptions) {
    return QueryProcessor.instance.parse(s, queryState, queryOptions);
  }

  @Override
  public ResultMessage process(
      CQLStatement statement,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {
    ResultMessage result =
        maybeIntercept(statement, queryState, options, customPayload, queryStartNanoTime);
    return result == null
        ? QueryProcessor.instance.process(
            statement, queryState, options, customPayload, queryStartNanoTime)
        : result;
  }

  @Override
  public ResultMessage.Prepared prepare(
      String s, ClientState clientState, Map<String, ByteBuffer> map)
      throws RequestValidationException {
    return QueryProcessor.instance.prepare(s, clientState, map);
  }

  @Override
  public Prepared getPrepared(MD5Digest md5Digest) {
    return QueryProcessor.instance.getPrepared(md5Digest);
  }

  @Override
  public ResultMessage processPrepared(
      CQLStatement statement,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {
    ResultMessage result =
        maybeIntercept(statement, queryState, options, customPayload, queryStartNanoTime);
    return result == null
        ? QueryProcessor.instance.processPrepared(
            statement, queryState, options, customPayload, queryStartNanoTime)
        : result;
  }

  @Override
  public ResultMessage processBatch(
      BatchStatement batchStatement,
      QueryState queryState,
      BatchQueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {
    return QueryProcessor.instance.processBatch(
        batchStatement, queryState, options, customPayload, queryStartNanoTime);
  }
}
