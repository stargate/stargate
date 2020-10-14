package io.stargate.db.cassandra.impl.interceptors;

import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isStargateNode;
import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemLocal;
import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemLocalOrPeers;

import com.google.common.collect.Sets;
import io.stargate.db.cassandra.impl.StargateSystemKeyspace;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * A default interceptor implementation that returns only stargate nodes for `system.peers` queries,
 * but also returns only the same, single token for all stargate nodes (they all own the whole ring)
 * for both `system.local` and `system.peers` tables.
 */
public class DefaultQueryInterceptor implements QueryInterceptor, IEndpointStateChangeSubscriber {

  private final List<IEndpointLifecycleSubscriber> subscribers = new CopyOnWriteArrayList<>();
  private final Set<InetAddress> liveStargateNodes = Sets.newConcurrentHashSet();

  @Override
  public void initialize() {
    Schema.instance.load(StargateSystemKeyspace.metadata());
    Gossiper.instance.register(new StargateSystemKeyspace.PeersUpdater());
    Gossiper.instance.register(this);
    StargateSystemKeyspace.persistLocalMetadata();
  }

  @Override
  public ResultMessage interceptQuery(
      CQLStatement statement,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {
    if (!isSystemLocalOrPeers(statement)) {
      return null;
    }

    SelectStatement selectStatement = (SelectStatement) statement;
    SelectStatement interceptStatement =
        new SelectStatement(
            isSystemLocal(selectStatement)
                ? StargateSystemKeyspace.Local
                : StargateSystemKeyspace.Peers,
            selectStatement.getBoundTerms(),
            selectStatement.parameters,
            selectStatement.getSelection(),
            selectStatement.getRestrictions(),
            false,
            null,
            null,
            null,
            null);
    ResultMessage.Rows rows = interceptStatement.execute(state, options, queryStartNanoTime);
    return new ResultMessage.Rows(
        new ResultSet(selectStatement.getResultMetadata(), rows.result.rows));
  }

  @Override
  public void register(IEndpointLifecycleSubscriber subscriber) {
    subscribers.add(subscriber);
  }

  @Override
  public void onJoin(InetAddress endpoint, EndpointState epState) {
    addStargateNode(endpoint, epState);
  }

  @Override
  public void beforeChange(
      InetAddress endpoint,
      EndpointState currentState,
      ApplicationState newStateKey,
      VersionedValue newValue) {}

  @Override
  public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
    EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
    if (epState == null || Gossiper.instance.isDeadState(epState)) {
      return;
    }

    addStargateNode(endpoint, epState);
  }

  @Override
  public void onAlive(InetAddress endpoint, EndpointState state) {}

  @Override
  public void onDead(InetAddress endpoint, EndpointState state) {}

  @Override
  public void onRemove(InetAddress endpoint) {
    if (!liveStargateNodes.remove(endpoint)) {
      return;
    }

    for (IEndpointLifecycleSubscriber subscriber : subscribers) subscriber.onLeaveCluster(endpoint);
  }

  @Override
  public void onRestart(InetAddress endpoint, EndpointState state) {}

  private void addStargateNode(InetAddress endpoint, EndpointState epState) {
    if (!isStargateNode(epState) || !liveStargateNodes.add(endpoint)) {
      return;
    }

    for (IEndpointLifecycleSubscriber subscriber : subscribers) subscriber.onJoinCluster(endpoint);
  }
}
