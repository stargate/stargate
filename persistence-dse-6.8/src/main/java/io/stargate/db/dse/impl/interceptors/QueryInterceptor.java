package io.stargate.db.dse.impl.interceptors;

import io.reactivex.Single;
import io.stargate.db.EventListener;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * An interface for intercepting queries and node lifecycle events. It's used to intercept
 * `system.local` and `system.peers` queries and topology events for stargate nodes.
 */
public interface QueryInterceptor {
  void initialize();

  Single<ResultMessage> interceptQuery(
      CQLStatement statement,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime);

  void register(EventListener listener);

  void unregister(EventListener listener);
}
