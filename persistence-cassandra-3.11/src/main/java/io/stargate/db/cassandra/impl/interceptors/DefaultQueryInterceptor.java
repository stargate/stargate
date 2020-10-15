package io.stargate.db.cassandra.impl.interceptors;

import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isStargateNode;
import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemLocal;
import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemLocalOrPeers;

import com.google.common.collect.Sets;
import io.stargate.db.EventListener;
import io.stargate.db.cassandra.impl.StargateSystemKeyspace;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.cassandra.config.DatabaseDescriptor;
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
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.stargate.locator.InetAddressAndPort;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default interceptor implementation that returns only stargate nodes for `system.peers` queries,
 * but also returns only the same, single token for all stargate nodes (they all own the whole ring)
 * for both `system.local` and `system.peers` tables.
 */
public class DefaultQueryInterceptor implements QueryInterceptor, IEndpointStateChangeSubscriber {
  private static final Logger logger = LoggerFactory.getLogger(DefaultQueryInterceptor.class);

  private final List<EventListener> listeners = new CopyOnWriteArrayList<>();
  private final Set<InetAddress> liveStargateNodes = Sets.newConcurrentHashSet();

  // We also want to delay delivering a NEW_NODE notification until the new node has set its RPC
  // ready state. This tracks the endpoints which have joined, but not yet signalled they're ready
  // for clients.
  private final Set<InetAddress> endpointsPendingJoinedNotification = ConcurrentHashMap.newKeySet();

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
  public void register(EventListener listener) {
    listeners.add(listener);
  }

  @Override
  public void onJoin(InetAddress endpoint, EndpointState epState) {
    if (!isStargateNode(epState)) {
      return;
    }
    maybeJoinCluster(endpoint);
  }

  @Override
  public void beforeChange(
      InetAddress endpoint,
      EndpointState currentState,
      ApplicationState newStateKey,
      VersionedValue newValue) {}

  @Override
  public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
    if (state == ApplicationState.STATUS) {
      return;
    }

    EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
    if (epState == null || Gossiper.instance.isDeadState(epState)) {
      return;
    }

    if (!isStargateNode(epState)) {
      return;
    }

    maybeJoinCluster(endpoint);

    if (state == ApplicationState.RPC_READY) {
      notifyRpcChange(endpoint);
    }
  }

  @Override
  public void onAlive(InetAddress endpoint, EndpointState state) {
    if (!isStargateNode(state)) {
      return;
    }
    notifyUp(endpoint);
  }

  @Override
  public void onDead(InetAddress endpoint, EndpointState state) {
    if (!isStargateNode(state)) {
      return;
    }
    notifyDown(endpoint);
  }

  @Override
  public void onRemove(InetAddress endpoint) {
    leaveCluster(endpoint);
  }

  @Override
  public void onRestart(InetAddress endpoint, EndpointState state) {}

  private void maybeJoinCluster(InetAddress endpoint) {
    if (StorageService.instance.isRpcReady(endpoint)) {
      joinCluster(endpoint);
    } else {
      endpointsPendingJoinedNotification.add(endpoint);
    }
  }

  private void joinCluster(InetAddress endpoint) {
    if (!liveStargateNodes.add(endpoint)) {
      return;
    }
    InetAddressAndPort nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onJoinCluster(nativeAddress);
    }
  }

  private void leaveCluster(InetAddress endpoint) {
    if (!liveStargateNodes.remove(endpoint)) {
      return;
    }
    InetAddressAndPort nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onLeaveCluster(nativeAddress);
    }
  }

  private void notifyRpcChange(InetAddress endpoint) {
    if (StorageService.instance.isRpcReady(endpoint)) {
      notifyUp(endpoint);
    } else {
      notifyDown(endpoint);
    }
  }

  private void notifyUp(InetAddress endpoint) {
    if (!StorageService.instance.isRpcReady(endpoint) || !Gossiper.instance.isAlive(endpoint)) {
      return;
    }

    if (endpointsPendingJoinedNotification.remove(endpoint)) {
      joinCluster(endpoint);
    }

    InetAddressAndPort nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onUp(nativeAddress);
    }
  }

  private void notifyDown(InetAddress endpoint) {
    InetAddressAndPort nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onDown(nativeAddress);
    }
  }

  private InetAddressAndPort getNativeAddress(InetAddress endpoint) {
    try {
      return InetAddressAndPort.getByName(StorageService.instance.getRpcaddress(endpoint));
    } catch (UnknownHostException e) {
      // That should not happen, so log an error, but return the
      // endpoint address since there's a good change this is right
      logger.error("Problem retrieving RPC address for {}", endpoint, e);
      return InetAddressAndPort.getByAddressOverrideDefaults(
          endpoint, DatabaseDescriptor.getNativeTransportPort());
    }
  }
}
