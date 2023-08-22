package io.stargate.db.cassandra.impl.interceptors;

import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemLocalOrPeers;
import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemPeers;
import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemPeersV2;

import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import io.stargate.db.EventListener;
import io.stargate.db.cassandra.impl.SelectStatementWithRawCql;
import io.stargate.db.cassandra.impl.StargatePeerInfo;
import io.stargate.db.cassandra.impl.StargateSystemKeyspace;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default interceptor implementation that returns only stargate nodes for `system.peers` queries.
 */
public class DefaultQueryInterceptor implements QueryInterceptor, IEndpointStateChangeSubscriber {
  private static final Logger logger = LoggerFactory.getLogger(DefaultQueryInterceptor.class);

  private final List<EventListener> listeners = new CopyOnWriteArrayList<>();
  private final Set<InetAddressAndPort> liveStargateNodes = Sets.newConcurrentHashSet();

  // We also want to delay delivering a NEW_NODE notification until the new node has set its RPC
  // ready state. This tracks the endpoints which have joined, but not yet signalled they're ready
  // for clients.
  private final Set<InetAddressAndPort> endpointsPendingJoinedNotification =
      ConcurrentHashMap.newKeySet();

  @Override
  public void initialize() {
    VirtualKeyspaceRegistry.instance.register(StargateSystemKeyspace.instance);
    Gossiper.instance.register(this);
    StargateSystemKeyspace.instance.persistLocalMetadata();
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
    TableMetadata tableMetadata =
        StargateSystemKeyspace.instance
            .metadata()
            .getTableNullable(StargateSystemKeyspace.LOCAL_TABLE_NAME);
    if (isSystemPeers(selectStatement))
      tableMetadata =
          StargateSystemKeyspace.instance
              .metadata()
              .getTableNullable(StargateSystemKeyspace.LEGACY_PEERS_TABLE_NAME);
    else if (isSystemPeersV2(selectStatement))
      tableMetadata =
          StargateSystemKeyspace.instance
              .metadata()
              .getTableNullable(StargateSystemKeyspace.PEERS_V2_TABLE_NAME);

    // Re-parse so that we can intercept and replace the keyspace.
    SelectStatementWithRawCql selectStatementWithRawCql =
        (SelectStatementWithRawCql) selectStatement;
    SelectStatement.RawStatement rawStatement =
        (SelectStatement.RawStatement)
            QueryProcessor.parseStatement(selectStatementWithRawCql.getRawCQLStatement());

    rawStatement.setKeyspace(StargateSystemKeyspace.SYSTEM_KEYSPACE_NAME);
    SelectStatement interceptStatement = rawStatement.prepare(state.getClientState());

    ResultMessage.Rows rows = interceptStatement.execute(state, options, queryStartNanoTime);
    return new ResultMessage.Rows(
        new ResultSet(selectStatement.getResultMetadata(), rows.result.rows));
  }

  @Override
  public void register(EventListener listener) {
    listeners.add(listener);
  }

  @Override
  public void onJoin(InetAddressAndPort endpoint, EndpointState state) {
    if (!isStargateNode(state)) {
      return;
    }

    joinCluster(endpoint, state);
  }

  @Override
  public void beforeChange(
      InetAddressAndPort endpoint,
      EndpointState currentState,
      ApplicationState newStateKey,
      VersionedValue newValue) {}

  @Override
  public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value) {
    if (state == ApplicationState.STATUS || state == ApplicationState.STATUS_WITH_PORT) {
      return;
    }

    EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
    if (epState == null || Gossiper.instance.isDeadState(epState)) {
      return;
    }

    if (!isStargateNode(epState)) {
      return;
    }

    if (!joinCluster(endpoint, epState)) {
      applyState(endpoint, state, value, epState);
    }
  }

  @Override
  public void onAlive(InetAddressAndPort endpoint, EndpointState state) {
    if (!isStargateNode(state)) {
      return;
    }
    notifyUp(endpoint);
  }

  @Override
  public void onDead(InetAddressAndPort endpoint, EndpointState state) {
    if (!isStargateNode(state)) {
      return;
    }
    notifyDown(endpoint);
  }

  @Override
  public void onRemove(InetAddressAndPort endpoint) {
    leaveCluster(endpoint);
  }

  @Override
  public void onRestart(InetAddressAndPort endpoint, EndpointState state) {}

  private boolean joinCluster(InetAddressAndPort endpoint, EndpointState state) {
    if (!liveStargateNodes.add(endpoint)) {
      return false;
    }

    for (Map.Entry<ApplicationState, VersionedValue> entry : state.states()) {
      applyState(endpoint, entry.getKey(), entry.getValue(), state);
    }

    if (StorageService.instance.isRpcReady(endpoint)) {
      notifyJoinCluster(endpoint);
    } else {
      endpointsPendingJoinedNotification.add(endpoint);
    }

    return true;
  }

  private void leaveCluster(InetAddressAndPort endpoint) {
    if (!liveStargateNodes.remove(endpoint)) {
      return;
    }
    StargateSystemKeyspace.instance.getPeers().remove(endpoint);
    InetAddressAndPort nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onLeaveCluster(nativeAddress.getAddress(), nativeAddress.getPort());
    }
  }

  private void notifyJoinCluster(InetAddressAndPort endpoint) {
    InetAddressAndPort nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onJoinCluster(nativeAddress.getAddress(), nativeAddress.getPort());
    }
  }

  private void notifyRpcChange(InetAddressAndPort endpoint, boolean isReady) {
    if (isReady) {
      notifyUp(endpoint);
    } else {
      notifyDown(endpoint);
    }
  }

  private void notifyUp(InetAddressAndPort endpoint) {
    if (!StorageService.instance.isRpcReady(endpoint) || !Gossiper.instance.isAlive(endpoint)) {
      return;
    }

    if (endpointsPendingJoinedNotification.remove(endpoint)) {
      notifyJoinCluster(endpoint);
    }

    InetAddressAndPort nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onUp(nativeAddress.getAddress(), nativeAddress.getPort());
    }
  }

  private void applyState(
      InetAddressAndPort endpoint,
      ApplicationState state,
      VersionedValue value,
      EndpointState epState) {
    switch (state) {
      case RELEASE_VERSION:
        updatePeer(endpoint, value.value, StargatePeerInfo::setReleaseVersion);
        break;
      case DC:
        updatePeer(endpoint, value.value, StargatePeerInfo::setDataCenter);
        break;
      case RACK:
        updatePeer(endpoint, value.value, StargatePeerInfo::setRack);
        break;
      case RPC_ADDRESS:
        try {
          updatePeer(
              endpoint, InetAddress.getByName(value.value), StargatePeerInfo::setNativeAddress);
        } catch (UnknownHostException e) {
          throw new RuntimeException(e);
        }
        break;
      case NATIVE_ADDRESS_AND_PORT:
        try {
          InetAddressAndPort address = InetAddressAndPort.getByName(value.value);
          updatePeer(endpoint, address.getAddress(), StargatePeerInfo::setNativeAddress);
          updatePeer(endpoint, address.getPort(), StargatePeerInfo::setNativePort);
        } catch (UnknownHostException e) {
          throw new RuntimeException(e);
        }
        break;
      case HOST_ID:
        updatePeer(endpoint, UUID.fromString(value.value), StargatePeerInfo::setHostId);
        break;
      case RPC_READY:
        notifyRpcChange(endpoint, epState.isRpcReady());
        break;
      default:
        // ignore other states
    }
  }

  private <V> void updatePeer(
      InetAddressAndPort endpoint, V value, BiConsumer<StargatePeerInfo, V> updater) {

    if (!endpoint.equals(FBUtilities.getBroadcastAddressAndPort())) {
      StargatePeerInfo peer =
          StargateSystemKeyspace.instance
              .getPeers()
              .computeIfAbsent(endpoint, StargatePeerInfo::new);
      updater.accept(peer, value);
    }
  }

  private void notifyDown(InetAddressAndPort endpoint) {
    InetAddressAndPort nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onDown(nativeAddress.getAddress(), nativeAddress.getPort());
    }
  }

  private InetAddressAndPort getNativeAddress(InetAddressAndPort endpoint) {
    try {
      return InetAddressAndPort.getByName(StorageService.instance.getNativeaddress(endpoint, true));
    } catch (UnknownHostException e) {
      // That should not happen, so log an error, but return the
      // endpoint address since there's a good change this is right
      logger.error("Problem retrieving RPC address for {}", endpoint, e);
      return endpoint;
    }
  }

  private static boolean isStargateNode(EndpointState epState) {
    VersionedValue value = epState.getApplicationState(ApplicationState.X10);
    return value != null && value.value.equals("stargate");
  }
}
