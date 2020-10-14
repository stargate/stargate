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
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Prepared;
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
  public ResultMessage process(
      String queryString,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {

    ParsedStatement.Prepared p =
        QueryProcessor.getStatement(queryString, queryState.getClientState());
    options.prepare(p.boundNames);
    CQLStatement statement = p.statement;
    if (statement.getBoundTerms() != options.getValues().size()) {
      throw new InvalidRequestException(
          String.format(
              "there were %d markers(?) in CQL but %d bound variables",
              statement.getBoundTerms(), options.getValues().size()));
    }

    if (!queryState.getClientState().isInternal)
      QueryProcessor.metrics.regularStatementsExecuted.inc();

    ResultMessage result =
        maybeIntercept(statement, queryState, options, customPayload, queryStartNanoTime);
    return result == null
        ? QueryProcessor.instance.processStatement(
            statement, queryState, options, queryStartNanoTime)
        : result;
  }

  @Override
  public Prepared prepare(String s, QueryState queryState, Map<String, ByteBuffer> customPayload)
      throws RequestValidationException {
    return QueryProcessor.instance.prepare(s, queryState, customPayload);
  }

  @Override
  public ParsedStatement.Prepared getPrepared(MD5Digest id) {
    return QueryProcessor.instance.getPrepared(id);
  }

  @Override
  public ParsedStatement.Prepared getPreparedForThrift(Integer id) {
    return QueryProcessor.instance.getPreparedForThrift(id);
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
