package io.stargate.db.dse.impl.interceptors;

import static io.stargate.db.dse.impl.StargateSystemKeyspace.isSystemLocalOrPeers;

import com.datastax.bdp.db.nodes.BootstrapState;
import com.datastax.bdp.db.nodes.virtual.LocalNodeSystemView;
import com.datastax.bdp.db.nodes.virtual.PeersSystemView;
import com.datastax.bdp.db.util.ProductVersion;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.reactivex.Single;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.Result;
import io.stargate.db.dse.impl.Conversion;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.ResultSet.ResultMetadata;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A query interceptor that echos back the public IPs from the proxy protocol from `system.local`.
 * The goal is to populate `system.peers` with A-records from a provided DNS name.
 */
public class ProxyProtocolQueryInterceptor implements QueryInterceptor {
  public static final String PROXY_DNS_NAME = System.getProperty("stargate.proxy_protocol.dns_name");
  public static final long RESOLVE_DELAY_SECS = Long.getLong("stargate.proxy_protocol.resolve_delay_secs", 10);

  private static final Logger logger = LoggerFactory.getLogger(ProxyProtocolQueryInterceptor.class);
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  public interface Resolver {
    Set<InetAddress> resolve(String name) throws UnknownHostException;
  }

  private final Resolver resolver;
  private final String proxyDnsName;
  private final long resolveDelaySecs;
  private final List<IEndpointLifecycleSubscriber> subscribers = new CopyOnWriteArrayList<>();
  private volatile Set<InetAddress> peers = Collections.emptySet();

  public ProxyProtocolQueryInterceptor() {
    this(new DefaultResolver(), PROXY_DNS_NAME, RESOLVE_DELAY_SECS);
  }

  @VisibleForTesting
  public ProxyProtocolQueryInterceptor(Resolver resolver, String proxyDnsName, long resolveDelaySecs) {
    this.resolver = resolver;
    this.proxyDnsName = proxyDnsName;
    this.resolveDelaySecs = resolveDelaySecs;
  }

  @Override
  public void initialize() {
    String ttl = Security.getProperty("networkaddress.cache.ttl");
    if (Strings.isNullOrEmpty(ttl)) {
      Security.setProperty("networkaddress.cache.ttl", "60");
    }
    resolvePeers();
  }

  @Override
  public Single<Result> interceptQuery(
      CQLStatement statement,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {
    if (!isSystemLocalOrPeers(statement)) {
      return null;
    }

    SelectStatement selectStatement = (SelectStatement) statement;
    ProtocolVersion version = Conversion.toInternal(options.getProtocolVersion());

    List<List<ByteBuffer>> rows;
    InetSocketAddress publicAddress = state.getClientState().getPublicAddress();
    if (publicAddress == null) {
      throw new RuntimeException("Unable to intercept proxy protocol system query without a valid public address");
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
          rows.add(buildRow(selectStatement.getResultMetadata(), peer, publicAddress.getPort()));
        }
      }
    } else {
      assert tableName.equals(LocalNodeSystemView.NAME);
      rows =
          Collections.singletonList(
              buildRow(
                  selectStatement.getResultMetadata(),
                  publicAddress.getAddress(),
                  publicAddress.getPort()));
    }

    ResultSet resultSet = new ResultSet(selectStatement.getResultMetadata(), rows);
    return Single.just(Conversion.toResult(new ResultMessage.Rows(resultSet), version));
  }

  @Override
  public void register(IEndpointLifecycleSubscriber subscriber) {
    subscribers.add(subscriber);
  }

  private void resolvePeers() {
    if (!Strings.isNullOrEmpty(PROXY_DNS_NAME)) {
      try {
        Set<InetAddress> resolved = resolver.resolve(proxyDnsName);

        if (!peers.equals(resolved)) {
          Sets.SetView<InetAddress> added = Sets.difference(resolved, peers);
          Sets.SetView<InetAddress> removed = Sets.difference(peers, resolved);

          for (IEndpointLifecycleSubscriber subscriber : subscribers) {
            for (InetAddress peer : added) {
              subscriber.onJoinCluster(peer);
            }
            for (InetAddress peer : removed) {
              subscriber.onLeaveCluster(peer);
            }
          }
          peers = resolved;
        }
      } catch (UnknownHostException e) {
        throw new RuntimeException("Unable to resolve DNS for proxy protocol peers table", e);
      }
      scheduler.schedule(this::resolvePeers, resolveDelaySecs, TimeUnit.SECONDS);
    }
  }

  private static ByteBuffer buildColumnValue(
      String name, InetAddress publicAddress, int publicPort) {
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
        return SetType.getInstance(UTF8Type.instance, false)
            .decompose(
                Collections.singleton(
                    DatabaseDescriptor.getPartitioner().getMinimumToken().toString()));
      case "native_transport_port": // Fallthrough intentional
      case "native_transport_port_ssl":
        return Int32Type.instance.decompose(publicPort);
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

  private static List<ByteBuffer> buildRow(
      ResultMetadata metadata, InetAddress publicAddress, int publicPort) {
    List<ByteBuffer> row = Lists.newArrayListWithCapacity(metadata.names.size());
    metadata.names.forEach(
        column -> row.add(buildColumnValue(column.name.toString(), publicAddress, publicPort)));
    return row;
  }

  private static class DefaultResolver implements Resolver {
    @Override
    public Set<InetAddress> resolve(String name) throws UnknownHostException {
      return Arrays.stream(InetAddress.getAllByName(name)).collect(Collectors.toSet());
    }
  }
}
