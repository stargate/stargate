package io.stargate.db.dse.impl.interceptors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.bdp.db.nodes.BootstrapState;
import com.datastax.bdp.db.util.ProductVersion;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.stargate.db.DefaultQueryOptions;
import io.stargate.db.QueryOptions;
import io.stargate.db.Result;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.Result.Rows;
import io.stargate.db.dse.impl.BaseDseTest;
import io.stargate.db.dse.impl.ClientStateWrapper;
import io.stargate.db.dse.impl.QueryStateWrapper;
import io.stargate.db.dse.impl.StargateSystemKeyspace;
import io.stargate.db.dse.impl.interceptors.ProxyProtocolQueryInterceptor.Resolver;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ProxyProtocolQueryInterceptorTest extends BaseDseTest {
  private static final InetAddress REMOTE_ADDRESS;
  private static final InetAddress PUBLIC_ADDRESS1;
  private static final InetAddress PUBLIC_ADDRESS2;
  private static final InetAddress PUBLIC_ADDRESS3;

  static {
    try {
      REMOTE_ADDRESS = InetAddress.getByName("192.0.2.1");
      PUBLIC_ADDRESS1 = InetAddress.getByName("192.51.100.1");
      PUBLIC_ADDRESS2 = InetAddress.getByName("192.51.100.2");
      PUBLIC_ADDRESS3 = InetAddress.getByName("192.51.100.3");
    } catch (UnknownHostException e) {
      throw new RuntimeException("Unable to initialize addresses", e);
    }
  }

  private final String PROXY_DNS_NAME = "stargate-test";

  private final InetSocketAddress REMOTE_SOCKET_ADDRESS =
      new InetSocketAddress(REMOTE_ADDRESS, 1234);
  private final QueryOptions QUERY_OPTIONS = DefaultQueryOptions.builder().build();

  private static final Map<String, AbstractType<?>> TYPES =
      ImmutableMap.<String, AbstractType<?>>builder()
          .put("key", UTF8Type.instance)
          .put("bootstrapped", UTF8Type.instance)
          .put("rpc_address", InetAddressType.instance)
          .put("peer", InetAddressType.instance)
          .put("preferred_ip", InetAddressType.instance)
          .put("broadcast_address", InetAddressType.instance)
          .put("native_transport_address", InetAddressType.instance)
          .put("listen_address", InetAddressType.instance)
          .put("cluster_name", UTF8Type.instance)
          .put("cql_version", UTF8Type.instance)
          .put("data_center", UTF8Type.instance)
          .put("host_id", UUIDType.instance)
          .put("native_protocol_version", UTF8Type.instance)
          .put("partitioner", UTF8Type.instance)
          .put("rack", UTF8Type.instance)
          .put("release_version", UTF8Type.instance)
          .put("schema_version", UUIDType.instance)
          .put("tokens", SetType.getInstance(UTF8Type.instance, false))
          .put("native_transport_port", Int32Type.instance)
          .put("native_transport_port_ssl", Int32Type.instance)
          .put("storage_port", Int32Type.instance)
          .put("storage_port_ssl", Int32Type.instance)
          .put("jmx_port", Int32Type.instance)
          .build();

  @ParameterizedTest
  @MethodSource("publicAddresses")
  public void systemLocalContainsThePublicAddress(InetAddress publicAddress) {
    ProxyProtocolQueryInterceptor interceptor = new ProxyProtocolQueryInterceptor();

    Rows result = (Rows) interceptQuery(interceptor, "SELECT * FROM system.local", publicAddress);
    assertThat(collect(result, "rpc_address")).containsExactlyInAnyOrder(publicAddress);
  }

  public static InetAddress[] publicAddresses() {
    return new InetAddress[] {PUBLIC_ADDRESS1, PUBLIC_ADDRESS2};
  }

  @ParameterizedTest
  @MethodSource("localAndPeers")
  public void systemPeersContainsTheOthers(InetAddress local, Set<InetAddress> peers)
      throws UnknownHostException {
    Resolver resolver = mock(Resolver.class);
    when(resolver.resolve(PROXY_DNS_NAME))
        .thenReturn(ImmutableSet.of(PUBLIC_ADDRESS1, PUBLIC_ADDRESS2, PUBLIC_ADDRESS3));

    ProxyProtocolQueryInterceptor interceptor =
        new ProxyProtocolQueryInterceptor(resolver, PROXY_DNS_NAME, 1);

    Rows result = (Rows) interceptQuery(interceptor, "SELECT * FROM system.peers", local);
    assertThat(collect(result, "rpc_address")).isEqualTo(peers);
  }

  public static Arguments[] localAndPeers() {
    return new Arguments[] {
      arguments(PUBLIC_ADDRESS1, ImmutableSet.of(PUBLIC_ADDRESS2, PUBLIC_ADDRESS3)),
      arguments(PUBLIC_ADDRESS2, ImmutableSet.of(PUBLIC_ADDRESS1, PUBLIC_ADDRESS3)),
      arguments(PUBLIC_ADDRESS3, ImmutableSet.of(PUBLIC_ADDRESS1, PUBLIC_ADDRESS2))
    };
  }

  @ParameterizedTest
  @MethodSource({"localValues", "sharedValues"})
  public void systemLocalValues(String name, Object value) {
    ProxyProtocolQueryInterceptor interceptor = new ProxyProtocolQueryInterceptor();

    Rows result = (Rows) interceptQuery(interceptor, "SELECT * FROM system.local", PUBLIC_ADDRESS1);
    assertThat(collect(result, name)).containsExactlyInAnyOrder(value);
  }

  @ParameterizedTest
  @MethodSource({"peersValues", "sharedValues"})
  public void systemPeersValues(String name, Object value) throws UnknownHostException {
    Resolver resolver = mock(Resolver.class);
    when(resolver.resolve(PROXY_DNS_NAME))
        .thenReturn(ImmutableSet.of(PUBLIC_ADDRESS1, PUBLIC_ADDRESS2));

    ProxyProtocolQueryInterceptor interceptor =
        new ProxyProtocolQueryInterceptor(resolver, PROXY_DNS_NAME, 1);

    Rows result = (Rows) interceptQuery(interceptor, "SELECT * FROM system.peers", PUBLIC_ADDRESS2);
    assertThat(collect(result, name)).containsExactlyInAnyOrder(value);
  }

  public static Arguments[] localValues() {
    return new Arguments[] {
      arguments("key", "local"),
      arguments("bootstrapped", BootstrapState.COMPLETED.toString()),
      arguments("cluster_name", DatabaseDescriptor.getClusterName()),
      arguments("cql_version", QueryProcessor.CQL_VERSION.toString()),
      arguments("broadcast_address", PUBLIC_ADDRESS1),
      arguments("listen_address", PUBLIC_ADDRESS1),
      arguments("partitioner", DatabaseDescriptor.getPartitioner().getClass().getName()),
      arguments("native_protocol_version", String.valueOf(ProtocolVersion.CURRENT.asInt()))
    };
  }

  public static Arguments[] peersValues() {
    return new Arguments[] {
      arguments("peer", PUBLIC_ADDRESS1), arguments("preferred_ip", PUBLIC_ADDRESS1)
    };
  }

  public static Arguments[] sharedValues() {
    return new Arguments[] {
      arguments("native_transport_address", PUBLIC_ADDRESS1),
      arguments("rpc_address", PUBLIC_ADDRESS1),
      arguments("data_center", DatabaseDescriptor.getLocalDataCenter()),
      arguments("host_id", UUID.nameUUIDFromBytes(PUBLIC_ADDRESS1.getAddress())),
      arguments("rack", DatabaseDescriptor.getLocalRack()),
      arguments("release_version", ProductVersion.getReleaseVersion().toString()),
      arguments("schema_version", StargateSystemKeyspace.SCHEMA_VERSION),
      arguments(
          "tokens",
          Collections.singleton(DatabaseDescriptor.getPartitioner().getMinimumToken().toString())),
      arguments("native_transport_port", 9042),
      arguments("native_transport_port_ssl", 9042),
      arguments("storage_port", DatabaseDescriptor.getStoragePort()),
      arguments("storage_port_ssl", DatabaseDescriptor.getSSLStoragePort()),
      arguments("jmx_port", DatabaseDescriptor.getJMXPort().orElse(null)),
    };
  }

  @Test
  public void addPeer() throws UnknownHostException {
    Resolver resolver = mock(Resolver.class);
    when(resolver.resolve(PROXY_DNS_NAME))
        .thenReturn(ImmutableSet.of(PUBLIC_ADDRESS1, PUBLIC_ADDRESS2))
        .thenReturn(ImmutableSet.of(PUBLIC_ADDRESS1, PUBLIC_ADDRESS2, PUBLIC_ADDRESS3));

    ProxyProtocolQueryInterceptor interceptor =
        new ProxyProtocolQueryInterceptor(resolver, PROXY_DNS_NAME, 1);

    Rows resultBefore =
        (Rows) interceptQuery(interceptor, "SELECT * FROM system.peers", PUBLIC_ADDRESS1);
    assertThat(collect(resultBefore, "rpc_address")).containsExactly(PUBLIC_ADDRESS2);

    await()
        .atMost(3, TimeUnit.SECONDS)
        .until(
            () -> {
              Rows resultAfter =
                  (Rows) interceptQuery(interceptor, "SELECT * FROM system.peers", PUBLIC_ADDRESS1);
              return collect(resultAfter, "rpc_address")
                  .equals(ImmutableSet.of(PUBLIC_ADDRESS2, PUBLIC_ADDRESS3));
            });
  }

  @Test
  public void removePeer() throws UnknownHostException {
    Resolver resolver = mock(Resolver.class);
    when(resolver.resolve(PROXY_DNS_NAME))
        .thenReturn(ImmutableSet.of(PUBLIC_ADDRESS1, PUBLIC_ADDRESS2, PUBLIC_ADDRESS3))
        .thenReturn(ImmutableSet.of(PUBLIC_ADDRESS1, PUBLIC_ADDRESS2));

    ProxyProtocolQueryInterceptor interceptor =
        new ProxyProtocolQueryInterceptor(resolver, PROXY_DNS_NAME, 1);

    Rows resultBefore =
        (Rows) interceptQuery(interceptor, "SELECT * FROM system.peers", PUBLIC_ADDRESS1);
    assertThat(collect(resultBefore, "rpc_address"))
        .containsExactlyInAnyOrder(PUBLIC_ADDRESS2, PUBLIC_ADDRESS3);

    await()
        .atMost(3, TimeUnit.SECONDS)
        .until(
            () -> {
              Rows resultAfter =
                  (Rows) interceptQuery(interceptor, "SELECT * FROM system.peers", PUBLIC_ADDRESS1);
              return collect(resultAfter, "rpc_address").equals(ImmutableSet.of(PUBLIC_ADDRESS2));
            });
  }

  private <T> Set<T> collect(Rows result, String name) {
    return result.rows.stream()
        .map(row -> (T) columnValue(row, result.resultMetadata, name))
        .collect(Collectors.toSet());
  }

  @SuppressWarnings({"unchecked"})
  private static <T> T columnValue(List<ByteBuffer> row, ResultMetadata metadata, String name) {
    OptionalInt index =
        IntStream.range(0, metadata.columnCount)
            .filter(i -> metadata.columns.get(i).name().equals(name))
            .findFirst();
    assertThat(index).isPresent();

    AbstractType<?> type = TYPES.get(name);
    assertThat(type).isNotNull();

    ByteBuffer value = row.get(index.getAsInt());
    return value == null ? null : (T) type.compose(value);
  }

  private QueryStateWrapper queryStateForAddress(InetAddress address) {
    return new QueryStateWrapper(
        ClientStateWrapper.forExternalCalls(
            null, REMOTE_SOCKET_ADDRESS, new InetSocketAddress(address, 9042)));
  }

  private Result interceptQuery(
      ProxyProtocolQueryInterceptor interceptor, String query, InetAddress publicAddress) {
    QueryStateWrapper queryState = queryStateForAddress(publicAddress);
    CQLStatement statement = QueryProcessor.parseStatement(query, queryState.getWrapped());
    interceptor.initialize();
    return interceptor.interceptQuery(statement, queryState, QUERY_OPTIONS, null, 0).blockingGet();
  }
}
