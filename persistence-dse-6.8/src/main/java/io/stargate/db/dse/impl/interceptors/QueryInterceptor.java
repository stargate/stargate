package io.stargate.db.dse.impl.interceptors;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;

import io.reactivex.Single;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.Result;

/**
 * An interface for intercepting queries and node lifecycle events. It's used to intercept `system.local`
 * and `system.peers` queries and topology events for stargate nodes.
 */
public interface QueryInterceptor
{
    void initialize();

    Single<Result> interceptQuery(QueryHandler handler,
                                  CQLStatement statement, QueryState state, QueryOptions options,
                                  Map<String, ByteBuffer> customPayload, long queryStartNanoTime);

    void register(IEndpointLifecycleSubscriber subscriber);
}
