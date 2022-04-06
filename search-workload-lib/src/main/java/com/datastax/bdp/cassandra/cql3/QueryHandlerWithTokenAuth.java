package com.datastax.bdp.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;

public interface QueryHandlerWithTokenAuth extends QueryHandler {
  void authorizeByToken(Map<String, ByteBuffer> customPayload, CQLStatement statement);
}
