package io.stargate.db.dse.impl.interceptors;

import static io.stargate.db.dse.impl.StargateSystemKeyspace.SYSTEM_KEYSPACE_NAME;
import static io.stargate.db.dse.impl.StargateSystemKeyspace.isSystemLocalOrPeers;

import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import io.reactivex.Single;
import io.stargate.db.EventListener;
import io.stargate.db.dse.impl.StargateClientState;
import io.stargate.db.dse.impl.StargatePeerInfo;
import io.stargate.db.dse.impl.StargateSystemKeyspace;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
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
  private static final ColumnIdentifier NATIVE_TRANSPORT_PORT_ID =
      new ColumnIdentifier("native_transport_port", false);
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  public static final long JOIN_NOTIFY_DELAY_SECS =
      Long.getLong("stargate.join_notify_delay_secs", 5);

  private final List<EventListener> listeners = new CopyOnWriteArrayList<>();
  private final Set<InetAddress> liveStargateNodes = Sets.newConcurrentHashSet();

  // We also want to delay delivering a NEW_NODE notification until the new node has set its RPC
  // ready state. This tracks the endpoints which have joined, but not yet signalled they're ready
  // for clients.
  private final Set<InetAddress> endpointsPendingJoinedNotification = ConcurrentHashMap.newKeySet();

  @Override
  public void initialize() {
    StargateSystemKeyspace.initialize();
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
        r -> {
          assert state.getClientState() instanceof StargateClientState; // see DseConnection
          StargateClientState clientState = (StargateClientState) state.getClientState();
          clientState.boundPort().ifPresent(port -> replaceNativeTransportPort(r.result, port));
          return new ResultMessage.Rows(
              new ResultSet(selectStatement.getResultMetadata(), r.result.rows));
        });
  }

  @Override
  public void onJoin(InetAddress endpoint, EndpointState state) {
    if (!isStargateNode(state)) {
      return;
    }

    joinCluster(endpoint, state);
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

    if (!joinCluster(endpoint, epState)) {
      applyState(endpoint, state, value, epState);
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

  private boolean joinCluster(InetAddress endpoint, EndpointState state) {
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

  private void leaveCluster(InetAddress endpoint) {
    if (!liveStargateNodes.remove(endpoint)) {
      return;
    }
    StargateSystemKeyspace.instance.getPeers().remove(endpoint);
    InetAddress nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onLeaveCluster(nativeAddress, EventListener.NO_PORT);
    }
  }

  private void notifyJoinCluster(InetAddress endpoint) {

    InetAddress nativeAddress = getNativeAddress(endpoint);

    scheduler.schedule(
        () -> {
          for (EventListener listener : listeners) {
            listener.onJoinCluster(nativeAddress, EventListener.NO_PORT);
          }
        },
        JOIN_NOTIFY_DELAY_SECS,
        TimeUnit.SECONDS);
  }

  private void notifyRpcChange(InetAddress endpoint, boolean isReady) {
    if (isReady) {
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
      notifyJoinCluster(endpoint);
    }

    InetAddress nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onUp(nativeAddress, EventListener.NO_PORT);
    }
  }

  private void applyState(
      InetAddress endpoint, ApplicationState state, VersionedValue value, EndpointState epState) {
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
      case NATIVE_TRANSPORT_ADDRESS:
        try {
          updatePeer(
              endpoint, InetAddress.getByName(value.value), StargatePeerInfo::setNativeAddress);
        } catch (UnknownHostException e) {
          throw new RuntimeException(e);
        }
        break;
      case NATIVE_TRANSPORT_PORT:
        updatePeer(endpoint, Integer.parseInt(value.value), StargatePeerInfo::setNativePort);
        break;
      case NATIVE_TRANSPORT_PORT_SSL:
        updatePeer(endpoint, Integer.parseInt(value.value), StargatePeerInfo::setNativePortSsl);
        break;
      case STORAGE_PORT:
        updatePeer(endpoint, Integer.parseInt(value.value), StargatePeerInfo::setStoragePort);
        break;
      case STORAGE_PORT_SSL:
        updatePeer(endpoint, Integer.parseInt(value.value), StargatePeerInfo::setStoragePortSsl);
        break;
      case JMX_PORT:
        updatePeer(endpoint, Integer.parseInt(value.value), StargatePeerInfo::setJmxPort);
        break;
      case HOST_ID:
        updatePeer(endpoint, UUID.fromString(value.value), StargatePeerInfo::setHostId);
        break;
      case NATIVE_TRANSPORT_READY:
        notifyRpcChange(endpoint, epState.isRpcReady());
        break;
      default:
        // ignore other states
    }
  }

  private <V> void updatePeer(
      InetAddress endpoint, V value, BiConsumer<StargatePeerInfo, V> updater) {
    if (!FBUtilities.getBroadcastAddress().equals(endpoint)) {
      StargatePeerInfo peer =
          StargateSystemKeyspace.instance
              .getPeers()
              .computeIfAbsent(endpoint, StargatePeerInfo::new);
      updater.accept(peer, value);
    }
  }

  private void notifyDown(InetAddress endpoint) {
    InetAddress nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onDown(nativeAddress, EventListener.NO_PORT);
    }
  }

  private InetAddress getNativeAddress(InetAddress endpoint) {
    try {
      return InetAddress.getByName(StorageService.instance.getRpcaddress(endpoint));
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

  private static void replaceNativeTransportPort(ResultSet rs, int port) {
    List<ColumnSpecification> columns = rs.metadata.names;
    int columnCount = rs.metadata.getColumnCount();

    int index = -1;
    for (int i = 0; i < columnCount; ++i) {
      if (columns.get(i).name.equals(NATIVE_TRANSPORT_PORT_ID)) {
        index = i;
      }
    }

    if (index == -1) {
      return;
    }

    ByteBuffer portBytes = Int32Type.instance.decompose(port);

    for (List<ByteBuffer> row : rs.rows) {
      row.set(index, portBytes);
    }
  }
}
