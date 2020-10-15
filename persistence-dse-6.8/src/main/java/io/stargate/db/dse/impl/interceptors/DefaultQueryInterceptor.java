package io.stargate.db.dse.impl.interceptors;

import static io.stargate.db.dse.impl.StargateSystemKeyspace.SYSTEM_KEYSPACE_NAME;
import static io.stargate.db.dse.impl.StargateSystemKeyspace.isStargateNode;
import static io.stargate.db.dse.impl.StargateSystemKeyspace.isSystemLocalOrPeers;

import com.google.common.collect.Sets;
import io.reactivex.Single;
import io.stargate.db.EventListener;
import io.stargate.db.dse.impl.StargateSystemKeyspace;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
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
    StargateSystemKeyspace.initialize();
    Gossiper.instance.register(StargateSystemKeyspace.instance.getPeersUpdater());
    Gossiper.instance.register(this);
    StargateSystemKeyspace.instance.persistLocalMetadata();
  }

  @Override
  public Single<ResultMessage> interceptQuery(
      CQLStatement statement,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {
    if (!isSystemLocalOrPeers(statement)) {
      return null;
    }
    return interceptSystemLocalOrPeers(statement, state, options, queryStartNanoTime);
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

    if (state == ApplicationState.NATIVE_TRANSPORT_READY) {
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

  private static Single<ResultMessage> interceptSystemLocalOrPeers(
      CQLStatement statement, QueryState state, QueryOptions options, long queryStartNanoTime) {
    SelectStatement selectStatement = ((SelectStatement) statement);

    // Re-parse so that we can intercept and replace the keyspace.
    SelectStatement.Raw rawStatement =
        (SelectStatement.Raw) QueryProcessor.parseStatement(selectStatement.queryString);
    rawStatement.setKeyspace(SYSTEM_KEYSPACE_NAME);

    SelectStatement interceptStatement = rawStatement.prepare(state.getClientState());
    Single<ResultMessage.Rows> rows =
        interceptStatement.execute(state, options, queryStartNanoTime);
    return rows.map(
        r ->
            new ResultMessage.Rows(
                new ResultSet(selectStatement.getResultMetadata(), r.result.rows)));
  }

  private static InetAddressAndPort getNativeAddress(InetAddress endpoint) {
    InetAddress nativeAddress;
    try {
      nativeAddress =
          InetAddress.getByName(StorageService.instance.getNativeTransportAddress(endpoint));
    } catch (UnknownHostException e) {
      // That should not happen, so log an error, but return the
      // endpoint address since there's a good change this is right
      logger.error("Problem retrieving RPC address for {}", endpoint, e);
      nativeAddress = endpoint;
    }
    return InetAddressAndPort.getByAddressOverrideDefaults(
        nativeAddress, DatabaseDescriptor.getNativeTransportPort());
  }
}
