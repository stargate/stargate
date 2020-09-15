package io.stargate.db.dse.impl.interceptors;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.transport.messages.ResultMessage;

import com.google.common.collect.Sets;
import io.reactivex.Single;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.Result;
import io.stargate.db.dse.impl.Conversion;
import io.stargate.db.dse.impl.StargateSystemKeyspace;

import static io.stargate.db.dse.impl.StargateSystemKeyspace.SYSTEM_KEYSPACE_NAME;
import static io.stargate.db.dse.impl.StargateSystemKeyspace.isStargateNode;
import static io.stargate.db.dse.impl.StargateSystemKeyspace.isSystemLocalOrPeers;

/**
 * A default interceptor implementation that returns only stargate nodes for `system.peers` queries, but also
 * returns only the same, single token for all stargate nodes (they all own the whole ring) for both `system.local`
 * and `system.peers` tables.
 */
public class DefaultQueryInterceptor implements QueryInterceptor, IEndpointStateChangeSubscriber
{
    private final List<IEndpointLifecycleSubscriber> subscribers = new CopyOnWriteArrayList<>();
    private final Set<InetAddress> liveStargateNodes = Sets.newConcurrentHashSet();

    @Override
    public void initialize()
    {
        StargateSystemKeyspace.initialize();
        Gossiper.instance.register(StargateSystemKeyspace.instance.getPeersUpdater());
        Gossiper.instance.register(this);
        StargateSystemKeyspace.instance.persistLocalMetadata();
    }

    @Override
    public Single<Result> interceptQuery(QueryHandler handler,
                                         CQLStatement statement, QueryState state, QueryOptions options,
                                         Map<String, ByteBuffer> customPayload, long queryStartNanoTime)
    {
        if (!isSystemLocalOrPeers(statement))
        {
            return null;
        }

        org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);
        org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);
        return interceptSystemLocalOrPeers(statement, internalState, internalOptions, queryStartNanoTime);
    }

    @Override
    public void register(IEndpointLifecycleSubscriber subscriber)
    {
        subscribers.add(subscriber);
    }

    @Override
    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        addStargateNode(endpoint, epState);
    }

    @Override
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {

    }

    @Override
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (epState == null || Gossiper.instance.isDeadState(epState))
        {
            return;
        }

        addStargateNode(endpoint, epState);
    }

    @Override
    public void onAlive(InetAddress endpoint, EndpointState state)
    {

    }

    @Override
    public void onDead(InetAddress endpoint, EndpointState state)
    {

    }

    @Override
    public void onRemove(InetAddress endpoint)
    {
        if (!liveStargateNodes.remove(endpoint))
        {
            return;
        }

        for (IEndpointLifecycleSubscriber subscriber : subscribers)
            subscriber.onLeaveCluster(endpoint);
    }

    @Override
    public void onRestart(InetAddress endpoint, EndpointState state)
    {

    }

    private void addStargateNode(InetAddress endpoint, EndpointState epState)
    {
        if (!isStargateNode(epState) || !liveStargateNodes.add(endpoint))
        {
            return;
        }

        for (IEndpointLifecycleSubscriber subscriber : subscribers)
            subscriber.onJoinCluster(endpoint);
    }

    private static Single<Result> interceptSystemLocalOrPeers(CQLStatement statement, org.apache.cassandra.service.QueryState state,
                                                              org.apache.cassandra.cql3.QueryOptions options,
                                                              long queryStartNanoTime)
    {
        SelectStatement selectStatement = ((SelectStatement) statement);

        // Re-parse so that we can intercept and replace the keyspace.
        SelectStatement.Raw rawStatement = (SelectStatement.Raw)QueryProcessor.parseStatement(selectStatement.queryString);
        rawStatement.setKeyspace(SYSTEM_KEYSPACE_NAME);

        SelectStatement interceptStatement = rawStatement.prepare(state.getClientState());
        Single<ResultMessage.Rows> rows = interceptStatement.execute(state, options, queryStartNanoTime);
        return rows.map(r ->
                Conversion.toResult(new ResultMessage.Rows(new ResultSet(selectStatement.getResultMetadata(), r.result.rows)), options.getProtocolVersion()));
    }
}
