package io.stargate.db.cassandra.impl.interceptors;

import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemLocal;
import static io.stargate.db.cassandra.impl.StargateSystemKeyspace.isSystemLocalOrPeers;

import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import io.stargate.db.EventListener;
import io.stargate.db.cassandra.impl.StargateSystemKeyspace;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
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
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default interceptor implementation that returns only stargate nodes for `system.peers` queries.
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

    updateTokens(endpoint);

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
    StargateSystemKeyspace.removeEndpoint(endpoint);
    InetAddress nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onLeaveCluster(nativeAddress, EventListener.NO_PORT);
    }
  }

  private void notifyJoinCluster(InetAddress endpoint) {
    InetAddress nativeAddress = getNativeAddress(endpoint);
    for (EventListener listener : listeners) {
      listener.onJoinCluster(nativeAddress, EventListener.NO_PORT);
    }
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
    final ExecutorService executor = StageManager.getStage(Stage.MUTATION);
    switch (state) {
      case RELEASE_VERSION:
        StargateSystemKeyspace.updatePeerInfo(endpoint, "release_version", value.value, executor);
        break;
      case DC:
        StargateSystemKeyspace.updatePeerInfo(endpoint, "data_center", value.value, executor);
        break;
      case RACK:
        StargateSystemKeyspace.updatePeerInfo(endpoint, "rack", value.value, executor);
        break;
      case RPC_ADDRESS:
        try {
          StargateSystemKeyspace.updatePeerInfo(
              endpoint, "rpc_address", InetAddress.getByName(value.value), executor);
        } catch (UnknownHostException e) {
          throw new RuntimeException(e);
        }
        break;
      case SCHEMA:
        // Use a fix schema version for all peers (always in agreement) because stargate waits
        // for DDL queries to reach agreement before returning.
        StargateSystemKeyspace.updatePeerInfo(
            endpoint, "schema_version", StargateSystemKeyspace.SCHEMA_VERSION, executor);
        break;
      case HOST_ID:
        StargateSystemKeyspace.updatePeerInfo(
            endpoint, "host_id", UUID.fromString(value.value), executor);
        break;
      case RPC_READY:
        notifyRpcChange(endpoint, epState.isRpcReady());
        break;
      default:
        // ignore other states
    }
  }

  private void updateTokens(InetAddress endpoint) {
    final ExecutorService executor = StageManager.getStage(Stage.MUTATION);
    StargateSystemKeyspace.updatePeerInfo(
        endpoint,
        "tokens",
        StargateSystemKeyspace.generateRandomTokens(endpoint, DatabaseDescriptor.getNumTokens()),
        executor);
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
      // endpoint address since there's a good chance this is right
      logger.error("Problem retrieving RPC address for {}", endpoint, e);
      return endpoint;
    }
  }

  private static boolean isStargateNode(EndpointState epState) {
    VersionedValue value = epState.getApplicationState(ApplicationState.X10);
    return value != null && value.value.equals("stargate");
  }
}
