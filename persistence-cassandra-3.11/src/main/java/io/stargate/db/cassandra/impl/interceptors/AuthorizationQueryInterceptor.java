package io.stargate.db.cassandra.impl.interceptors;

import io.stargate.db.EventListener;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthorizationQueryInterceptor implements QueryInterceptor {

  private static final Logger logger = LoggerFactory.getLogger(AuthorizationQueryInterceptor.class);

  @Override
  public void initialize() {}

  @Override
  public ResultMessage interceptQuery(
      CQLStatement statement,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {
    return null;
  }

  @Override
  public void register(EventListener listener) {}
}
