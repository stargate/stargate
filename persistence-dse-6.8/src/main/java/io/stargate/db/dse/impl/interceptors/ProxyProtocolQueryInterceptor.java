package io.stargate.db.dse.impl.interceptors;

import static io.stargate.db.dse.impl.StargateSystemKeyspace.isSystemLocalOrPeers;

import com.datastax.bdp.db.nodes.BootstrapState;
import com.datastax.bdp.db.nodes.virtual.LocalNodeSystemView;
import com.datastax.bdp.db.nodes.virtual.PeersSystemView;
import com.datastax.bdp.db.util.ProductVersion;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.reactivex.Single;
import io.stargate.db.EventListener;
import io.stargate.db.dse.impl.ClientStateWithPublicAddress;
import io.stargate.db.dse.impl.StargateSystemKeyspace;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.ResultSet.ResultMetadata;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.stargate.transport.ServerError;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A query interceptor that echos back the public IPs from the proxy protocol from `system.local`.
 * `system.peers` is populated with A-records from a provided DNS name.
 */
public class ProxyProtocolQueryInterceptor implements QueryInterceptor {
  public static final String PROXY_DNS_NAME =
      System.getProperty("stargate.proxy_protocol.dns_name");
  public static final int PROXY_PORT =
      Integer.getInteger(
          "stargate.proxy_protocol.port", DatabaseDescriptor.getNativeTransportPort());
  public static final long RESOLVE_DELAY_SECS =
      Long.getLong("stargate.proxy_protocol.resolve_delay_secs", 10);

  private static final Logger logger = LoggerFactory.getLogger(ProxyProtocolQueryInterceptor.class);
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private final Resolver resolver;
  private final String proxyDnsName;
  private final int proxyPort;
  private final long resolveDelaySecs;
  private final List<EventListener> listeners = new CopyOnWriteArrayList<>();
  private final Map<InetAddress, Set<String>> tokensCache = new ConcurrentHashMap<>();
  private volatile Set<InetAddress> peers = Collections.emptySet();

  /**
   * A class that takes a name and resolves {@link InetAddress}s. This interface exists to
   * facilitate testing.
   */
  public interface Resolver {

    /**
     * Resolve {@link InetAddress}s from a provided name.
     *
     * @param name
     * @return Resolved address for the name.
     * @throws UnknownHostException
     */
    Set<InetAddress> resolve(String name) throws UnknownHostException;
  }

  public ProxyProtocolQueryInterceptor() {
    this(new DefaultResolver(), PROXY_DNS_NAME, PROXY_PORT, RESOLVE_DELAY_SECS);
  }

  @VisibleForTesting
  public ProxyProtocolQueryInterceptor(
      Resolver resolver, String proxyDnsName, int proxyPort, long resolveDelaySecs) {
    this.resolver = resolver;
    this.proxyDnsName = proxyDnsName;
    this.proxyPort = proxyPort;
    this.resolveDelaySecs = resolveDelaySecs;
  }

  @Override
  public void initialize() {
    String ttl = Security.getProperty("networkaddress.cache.ttl");
    if (Strings.isNullOrEmpty(ttl)) {
      logger.info(
          "DNS cache TTL (property \"networkaddress.cache.ttl\") not explicitly set. Setting to 60 seconds.");
      Security.setProperty("networkaddress.cache.ttl", "60");
    }
    resolvePeers();
  }

  @Override
  public Single<ResultMessage> interceptQuery(
      CQLStatement statement,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {

    ClientState clientState = state.getClientState();
    if (!isSystemLocalOrPeers(statement)
        || !(clientState instanceof ClientStateWithPublicAddress)) {
      return null;
    }

    SelectStatement selectStatement = (SelectStatement) statement;

    List<List<ByteBuffer>> rows;
    InetSocketAddress publicAddress = ((ClientStateWithPublicAddress) clientState).publicAddress();
    if (publicAddress == null) {
      throw new ServerError(
          "Unable to intercept proxy protocol system query without a valid public address");
    }
    String tableName = selectStatement.table();
    if (tableName.equals(PeersSystemView.NAME)) {
      Set<InetAddress> currentPeers = peers;
      rows =
          currentPeers.isEmpty()
              ? Collections.emptyList()
              : Lists.newArrayListWithCapacity(currentPeers.size() - 1);
      for (InetAddress peer : currentPeers) {
        if (!peer.equals(publicAddress.getAddress())) {
          rows.add(buildRow(selectStatement.getResultMetadata(), peer));
        }
      }
    } else {
      assert tableName.equals(LocalNodeSystemView.NAME);
      rows =
          Collections.singletonList(
              buildRow(selectStatement.getResultMetadata(), publicAddress.getAddress()));
    }

    ResultSet resultSet = new ResultSet(selectStatement.getResultMetadata(), rows);
    return Single.just(new ResultMessage.Rows(resultSet));
  }

  @Override
  public void register(EventListener listener) {
    listeners.add(listener);
  }

  private void resolvePeers() {
    if (!Strings.isNullOrEmpty(proxyDnsName)) {
      try {
        Set<InetAddress> resolved = resolver.resolve(proxyDnsName);

        if (!peers.equals(resolved)) {
          // Generate listener events based on the differences between this and the previous
          // resolved peers.
          Sets.SetView<InetAddress> added = Sets.difference(resolved, peers);
          Sets.SetView<InetAddress> removed = Sets.difference(peers, resolved);

          for (EventListener listener : listeners) {
            for (InetAddress peer : added) {
              listener.onJoinCluster(peer, proxyPort);
              listener.onUp(peer, proxyPort);
            }
            for (InetAddress peer : removed) {
              tokensCache.remove(peer);
              listener.onLeaveCluster(peer, proxyPort);
            }
          }
          peers = resolved;
        }
      } catch (UnknownHostException e) {
        throw new ServerError("Unable to resolve DNS for proxy protocol peers table", e);
      }
      scheduler.schedule(this::resolvePeers, resolveDelaySecs, TimeUnit.SECONDS);
    }
  }

  /**
   * Creates {@link ByteBuffer} value for a given column name. Unhandled names and aggregates return
   * a null value.
   *
   * @param name
   * @param publicAddress
   * @return a {@link ByteBuffer} value for a given system local/peers column.
   */
  private ByteBuffer buildColumnValue(String name, InetAddress publicAddress) {
    switch (name) {
      case "key":
        return UTF8Type.instance.decompose("local");
      case "bootstrapped":
        return UTF8Type.instance.decompose(BootstrapState.COMPLETED.toString());
      case "peer": // Fallthrough intentional
      case "preferred_ip":
      case "broadcast_address":
      case "native_transport_address":
      case "listen_address":
      case "rpc_address":
        return InetAddressType.instance.decompose(publicAddress);
      case "cluster_name":
        return UTF8Type.instance.decompose(DatabaseDescriptor.getClusterName());
      case "cql_version":
        return UTF8Type.instance.decompose(QueryProcessor.CQL_VERSION.toString());
      case "data_center":
        return UTF8Type.instance.decompose(DatabaseDescriptor.getLocalDataCenter());
      case "host_id":
        return UUIDType.instance.decompose(UUID.nameUUIDFromBytes(publicAddress.getAddress()));
      case "native_protocol_version":
        return UTF8Type.instance.decompose(String.valueOf(ProtocolVersion.CURRENT.asInt()));
      case "partitioner":
        return UTF8Type.instance.decompose(
            DatabaseDescriptor.getPartitioner().getClass().getName());
      case "rack":
        return UTF8Type.instance.decompose(DatabaseDescriptor.getLocalRack());
      case "release_version":
        return UTF8Type.instance.decompose(ProductVersion.getReleaseVersion().toString());
      case "schema_version":
        return UUIDType.instance.decompose(StargateSystemKeyspace.SCHEMA_VERSION);
      case "tokens":
        return SetType.getInstance(UTF8Type.instance, false).decompose(getTokens(publicAddress));
      case "native_transport_port": // Fallthrough intentional
      case "native_transport_port_ssl":
        return Int32Type.instance.decompose(PROXY_PORT);
      case "storage_port":
        return Int32Type.instance.decompose(DatabaseDescriptor.getStoragePort());
      case "storage_port_ssl":
        return Int32Type.instance.decompose(DatabaseDescriptor.getSSLStoragePort());
      case "jmx_port":
        return DatabaseDescriptor.getJMXPort()
            .map(p -> Int32Type.instance.decompose(p))
            .orElse(null);
      default:
        return null;
    }
  }

  /**
   * Get tokens generated for a public address.
   *
   * <p>This caches generated tokens for known peer addresses; otherwise, it re-calculates tokens
   * every call for unknown addresses. This prevents {@code tokensCache} from growing without bound
   * as {@code publicAddress} is provided by the client (proxy). This could use something like
   * {@link LoadingCache} with maximum size, but then that requires exposing yet another
   * configuration value to users.
   *
   * @param publicAddress
   * @return a list of random token calculated using the the public address as a seed.
   */
  private Set<String> getTokens(InetAddress publicAddress) {
    if (peers.contains(publicAddress)) {
      return tokensCache.computeIfAbsent(
          publicAddress,
          pa -> StargateSystemKeyspace.generateRandomTokens(pa, DatabaseDescriptor.getNumTokens()));
    } else {
      return StargateSystemKeyspace.generateRandomTokens(
          publicAddress, DatabaseDescriptor.getNumTokens());
    }
  }

  /**
   * Builds a row using the {@link CQLStatement}'s result metadata. This doesn't handles special
   * cases like aggregates, null is returned in those cases, but it should be good enough for
   * handling system tables.
   *
   * @param metadata
   * @param publicAddress
   * @return a list of {@link ByteBuffer} values for a system local/peers row.
   */
  private List<ByteBuffer> buildRow(ResultMetadata metadata, InetAddress publicAddress) {
    List<ByteBuffer> row = Lists.newArrayListWithCapacity(metadata.names.size());
    metadata.names.forEach(
        column -> row.add(buildColumnValue(column.name.toString(), publicAddress)));
    return row;
  }

  private static class DefaultResolver implements Resolver {
    @Override
    public Set<InetAddress> resolve(String name) throws UnknownHostException {
      return Arrays.stream(InetAddress.getAllByName(name)).collect(Collectors.toSet());
    }
  }
}
