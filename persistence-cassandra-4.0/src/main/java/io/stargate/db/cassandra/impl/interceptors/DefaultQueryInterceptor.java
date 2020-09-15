package io.stargate.db.cassandra.impl.interceptors;

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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.transport.messages.ResultMessage;

import com.google.common.collect.Sets;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.Result;
import io.stargate.db.cassandra.impl.Conversion;
import io.stargate.db.cassandra.impl.StargateSystemKeyspace;

import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isStargateNode;
import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemLocalOrPeers;
import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemPeers;
import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemPeersV2;

/**
 * A default interceptor implementation that returns only stargate nodes for `system.peers` queries, but also
 * returns only the same, single token for all stargate nodes (they all own the whole ring) for both `system.local`
 * and `system.peers` tables.
 */
public class DefaultQueryInterceptor implements QueryInterceptor, IEndpointStateChangeSubscriber
{
    private final List<IEndpointLifecycleSubscriber> subscribers = new CopyOnWriteArrayList<>();
    private final Set<InetAddressAndPort> liveStargateNodes = Sets.newConcurrentHashSet();

    @Override
    public void initialize()
    {
        Schema.instance.load(StargateSystemKeyspace.metadata());
        Gossiper.instance.register(new StargateSystemKeyspace.PeersUpdater());
        Gossiper.instance.register(this);
        StargateSystemKeyspace.persistLocalMetadata();
    }

    @Override
    public Result interceptQuery(QueryHandler handler, CQLStatement statement,
                               QueryState state, QueryOptions options,
                               Map<String, ByteBuffer> customPayload, long queryStartNanoTime)
    {
        if (!isSystemLocalOrPeers(statement))
        {
            return null;
        }

        org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);
        org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);
        return Conversion.toResult(interceptSystemLocalOrPeers(statement, internalState, internalOptions, queryStartNanoTime), internalOptions.getProtocolVersion());
    }

    @Override
    public void register(IEndpointLifecycleSubscriber subscriber)
    {
        subscribers.add(subscriber);
    }

    @Override
    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
        addStargateNode(endpoint, epState);
    }

    @Override
    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {

    }

    @Override
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (epState == null || Gossiper.instance.isDeadState(epState))
        {
            return;
        }

        addStargateNode(endpoint, epState);
    }

    @Override
    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {

    }

    @Override
    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {

    }

    @Override
    public void onRemove(InetAddressAndPort endpoint)
    {
        if (!liveStargateNodes.remove(endpoint))
        {
            return;
        }

        for (IEndpointLifecycleSubscriber subscriber : subscribers)
            subscriber.onLeaveCluster(endpoint);
    }

    @Override
    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {

    }

    private void addStargateNode(InetAddressAndPort endpoint, EndpointState epState)
    {
        if (!isStargateNode(epState) || !liveStargateNodes.add(endpoint))
        {
            return;
        }

        for (IEndpointLifecycleSubscriber subscriber : subscribers)
            subscriber.onJoinCluster(endpoint);
    }

    private static ResultMessage.Rows interceptSystemLocalOrPeers(CQLStatement statement, org.apache.cassandra.service.QueryState state, org.apache.cassandra.cql3.QueryOptions options, long queryStartNanoTime)
    {
        SelectStatement selectStatement = (SelectStatement) statement;
        TableMetadata tableMetadata = StargateSystemKeyspace.Local;
        if (isSystemPeers(selectStatement))
            tableMetadata = StargateSystemKeyspace.Peers;
        else if (isSystemPeersV2(selectStatement))
            tableMetadata = StargateSystemKeyspace.PeersV2;
        SelectStatement interceptStatement =
                new SelectStatement(tableMetadata,
                        selectStatement.bindVariables,
                        selectStatement.parameters,
                        selectStatement.getSelection(),
                        selectStatement.getRestrictions(),
                        false,
                        null,
                        null,
                        null,
                        null);
        ResultMessage.Rows rows = interceptStatement.execute(state, options, queryStartNanoTime);
        return new ResultMessage.Rows(new ResultSet(selectStatement.getResultMetadata(), rows.result.rows));
    }
}
