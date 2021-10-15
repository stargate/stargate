package io.stargate.db.dse.impl.interceptors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.bdp.db.nodes.BootstrapState;
import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import io.stargate.db.ClientInfo;
import io.stargate.db.Result;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.Result.Rows;
import io.stargate.db.dse.impl.BaseDseTest;
import io.stargate.db.dse.impl.Conversion;
import io.stargate.db.dse.impl.StargateClientState;
import io.stargate.db.dse.impl.StargateSystemKeyspace;
import io.stargate.db.dse.impl.interceptors.ProxyProtocolQueryInterceptor.Resolver;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ProxyProtocolQueryInterceptorTest extends BaseDseTest {

  // The peer addresses we return for public connections
  private static final InetAddress PUBLIC_PEER1 = getRawAddress(1, 1, 1, 1);
  private static final InetAddress PUBLIC_PEER2 = getRawAddress(1, 1, 1, 2);
  private static final InetAddress PUBLIC_PEER3 = getRawAddress(1, 1, 1, 3);

  // The peer addresses we return for private connections
  private static final InetAddress PRIVATE_PEER1 = getRawAddress(172, 28, 0, 1);
  private static final InetAddress PRIVATE_PEER2 = getRawAddress(172, 28, 0, 2);
  private static final InetAddress PRIVATE_PEER3 = getRawAddress(172, 28, 0, 3);

  // A source address used for public connection tests (the actual value doesn't matter):
  private static final InetSocketAddress PUBLIC_SOURCE =
      new InetSocketAddress(getRawAddress(192, 0, 0, 1), 1234);

  // A destination address used for private connection tests (the actual value doesn't matter, it
  // just needs to be a private address).
  private static final InetAddress PRIVATE_DEST = getRawAddress(172, 28, 0, 4);

  private static final String PROXY_DNS_NAME = "stargate-test";
  private static final String INTERNAL_PROXY_DNS_NAME = "internal-stargate-test";
  private static final int PROXY_PORT = 9042;

  private static final Resolver DEFAULT_RESOLVER =
      name -> {
        if (PROXY_DNS_NAME.equals(name)) {
          return ImmutableSet.of(PUBLIC_PEER1, PUBLIC_PEER2, PUBLIC_PEER3);
        } else if (INTERNAL_PROXY_DNS_NAME.equals(name)) {
          return ImmutableSet.of(PRIVATE_PEER1, PRIVATE_PEER2, PRIVATE_PEER3);
        } else {
          throw new IllegalArgumentException("Not one of our mocked DNS names: " + name);
        }
      };

  private static final ImmutableMap<String, AbstractType<?>> TYPES =
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
  @MethodSource("sourceDestinationAndExpectedLocal")
  public void systemLocalContainsTheSourceOrDestinationAddress(
      InetSocketAddress source, InetAddress destination, InetAddress expectedLocal) {
    ProxyProtocolQueryInterceptor interceptor =
        new ProxyProtocolQueryInterceptor(
            DEFAULT_RESOLVER, PROXY_DNS_NAME, INTERNAL_PROXY_DNS_NAME, PROXY_PORT, 1);

    Rows result =
        (Rows) interceptQuery(interceptor, "SELECT * FROM system.local", source, destination);
    assertThat(collect(result, "rpc_address")).containsOnly(expectedLocal);
  }

  public static Arguments[] sourceDestinationAndExpectedLocal() {
    return new Arguments[] {
      arguments(PUBLIC_SOURCE, PUBLIC_PEER1, PUBLIC_PEER1),
      arguments(PUBLIC_SOURCE, PUBLIC_PEER2, PUBLIC_PEER2),
      arguments(PUBLIC_SOURCE, PUBLIC_PEER3, PUBLIC_PEER3),
      // If the destination is private, the system.local address is passed in the source, not the
      // destination.
      arguments(new InetSocketAddress(PRIVATE_PEER1, 9042), PRIVATE_DEST, PRIVATE_PEER1),
      arguments(new InetSocketAddress(PRIVATE_PEER2, 9042), PRIVATE_DEST, PRIVATE_PEER2),
      arguments(new InetSocketAddress(PRIVATE_PEER3, 9042), PRIVATE_DEST, PRIVATE_PEER3),
      arguments(new InetSocketAddress(PUBLIC_PEER1, 9042), PRIVATE_DEST, PUBLIC_PEER1),
      arguments(new InetSocketAddress(PUBLIC_PEER2, 9042), PRIVATE_DEST, PUBLIC_PEER2),
      arguments(new InetSocketAddress(PUBLIC_PEER3, 9042), PRIVATE_DEST, PUBLIC_PEER3),
    };
  }

  @ParameterizedTest
  @MethodSource("sourceDestinationAndExpectedPeers")
  public void systemPeersContainsTheOthers(
      InetSocketAddress source, InetAddress destination, Set<InetAddress> expectedPeers) {
    ProxyProtocolQueryInterceptor interceptor =
        new ProxyProtocolQueryInterceptor(
            DEFAULT_RESOLVER, PROXY_DNS_NAME, INTERNAL_PROXY_DNS_NAME, PROXY_PORT, 1);

    Rows result =
        (Rows) interceptQuery(interceptor, "SELECT * FROM system.peers", source, destination);
    assertThat(collect(result, "rpc_address")).isEqualTo(expectedPeers);
  }

  public static Arguments[] sourceDestinationAndExpectedPeers() {
    return new Arguments[] {
      arguments(PUBLIC_SOURCE, PUBLIC_PEER1, ImmutableSet.of(PUBLIC_PEER2, PUBLIC_PEER3)),
      arguments(PUBLIC_SOURCE, PUBLIC_PEER2, ImmutableSet.of(PUBLIC_PEER1, PUBLIC_PEER3)),
      arguments(PUBLIC_SOURCE, PUBLIC_PEER3, ImmutableSet.of(PUBLIC_PEER1, PUBLIC_PEER2)),
      // If the destination is private, the system.local address is passed in the source, and the
      // peers match its private/public nature.
      arguments(
          new InetSocketAddress(PRIVATE_PEER1, 9042),
          PRIVATE_DEST,
          ImmutableSet.of(PRIVATE_PEER2, PRIVATE_PEER3)),
      arguments(
          new InetSocketAddress(PRIVATE_PEER2, 9042),
          PRIVATE_DEST,
          ImmutableSet.of(PRIVATE_PEER1, PRIVATE_PEER3)),
      arguments(
          new InetSocketAddress(PRIVATE_PEER3, 9042),
          PRIVATE_DEST,
          ImmutableSet.of(PRIVATE_PEER1, PRIVATE_PEER2)),
      arguments(
          new InetSocketAddress(PUBLIC_PEER1, 9042),
          PRIVATE_DEST,
          ImmutableSet.of(PUBLIC_PEER2, PUBLIC_PEER3)),
      arguments(
          new InetSocketAddress(PUBLIC_PEER2, 9042),
          PRIVATE_DEST,
          ImmutableSet.of(PUBLIC_PEER1, PUBLIC_PEER3)),
      arguments(
          new InetSocketAddress(PUBLIC_PEER3, 9042),
          PRIVATE_DEST,
          ImmutableSet.of(PUBLIC_PEER1, PUBLIC_PEER2)),
    };
  }

  @ParameterizedTest
  @MethodSource({"localValues", "sharedValues"})
  public void systemLocalValues(String name, Object value) {
    ProxyProtocolQueryInterceptor interceptor =
        new ProxyProtocolQueryInterceptor(
            DEFAULT_RESOLVER, INTERNAL_PROXY_DNS_NAME, PROXY_DNS_NAME, PROXY_PORT, 1);

    Rows result =
        (Rows)
            interceptQuery(interceptor, "SELECT * FROM system.local", PUBLIC_SOURCE, PUBLIC_PEER1);
    assertThat(collect(result, name)).containsExactlyInAnyOrder(value);
  }

  @ParameterizedTest
  @MethodSource({"peersValues", "sharedValues"})
  public void systemPeersValues(String name, Object value) throws UnknownHostException {
    Resolver resolver = mock(Resolver.class);
    when(resolver.resolve(PROXY_DNS_NAME)).thenReturn(ImmutableSet.of(PUBLIC_PEER1, PUBLIC_PEER2));

    ProxyProtocolQueryInterceptor interceptor =
        new ProxyProtocolQueryInterceptor(
            resolver, PROXY_DNS_NAME, INTERNAL_PROXY_DNS_NAME, PROXY_PORT, 1);

    Rows result =
        (Rows)
            interceptQuery(interceptor, "SELECT * FROM system.peers", PUBLIC_SOURCE, PUBLIC_PEER2);
    assertThat(collect(result, name)).containsExactlyInAnyOrder(value);
  }

  public static Arguments[] localValues() {
    return new Arguments[] {
      arguments("key", "local"),
      arguments("bootstrapped", BootstrapState.COMPLETED.toString()),
      arguments("cluster_name", DatabaseDescriptor.getClusterName()),
      arguments("cql_version", QueryProcessor.CQL_VERSION.toString()),
      arguments("broadcast_address", PUBLIC_PEER1),
      arguments("listen_address", PUBLIC_PEER1),
      arguments("partitioner", DatabaseDescriptor.getPartitioner().getClass().getName()),
      arguments("native_protocol_version", String.valueOf(ProtocolVersion.CURRENT.asInt()))
    };
  }

  public static Arguments[] peersValues() {
    return new Arguments[] {
      arguments("peer", PUBLIC_PEER1), arguments("preferred_ip", PUBLIC_PEER1)
    };
  }

  public static Arguments[] sharedValues() {
    return new Arguments[] {
      arguments("native_transport_address", PUBLIC_PEER1),
      arguments("rpc_address", PUBLIC_PEER1),
      arguments("data_center", DatabaseDescriptor.getLocalDataCenter()),
      arguments("host_id", UUID.nameUUIDFromBytes(PUBLIC_PEER1.getAddress())),
      arguments("rack", DatabaseDescriptor.getLocalRack()),
      arguments("release_version", ProductVersion.getReleaseVersion().toString()),
      arguments("schema_version", StargateSystemKeyspace.SCHEMA_VERSION),
      arguments(
          "tokens",
          StargateSystemKeyspace.generateRandomTokens(
              PUBLIC_PEER1, DatabaseDescriptor.getNumTokens())),
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
        .thenReturn(ImmutableSet.of(PUBLIC_PEER1, PUBLIC_PEER2))
        .thenReturn(ImmutableSet.of(PUBLIC_PEER1, PUBLIC_PEER2, PUBLIC_PEER3));

    ProxyProtocolQueryInterceptor interceptor =
        new ProxyProtocolQueryInterceptor(
            resolver, PROXY_DNS_NAME, INTERNAL_PROXY_DNS_NAME, PROXY_PORT, 1);

    Rows resultBefore =
        (Rows)
            interceptQuery(interceptor, "SELECT * FROM system.peers", PUBLIC_SOURCE, PUBLIC_PEER1);
    assertThat(collect(resultBefore, "rpc_address")).containsExactly(PUBLIC_PEER2);

    await()
        .atMost(Duration.ofSeconds(3))
        .until(
            () -> {
              Rows resultAfter =
                  (Rows)
                      interceptQuery(
                          interceptor, "SELECT * FROM system.peers", PUBLIC_SOURCE, PUBLIC_PEER1);
              return collect(resultAfter, "rpc_address")
                  .equals(ImmutableSet.of(PUBLIC_PEER2, PUBLIC_PEER3));
            });
  }

  @Test
  public void removePeer() throws UnknownHostException {
    Resolver resolver = mock(Resolver.class);
    when(resolver.resolve(PROXY_DNS_NAME))
        .thenReturn(ImmutableSet.of(PUBLIC_PEER1, PUBLIC_PEER2, PUBLIC_PEER3))
        .thenReturn(ImmutableSet.of(PUBLIC_PEER1, PUBLIC_PEER2));

    ProxyProtocolQueryInterceptor interceptor =
        new ProxyProtocolQueryInterceptor(
            resolver, PROXY_DNS_NAME, INTERNAL_PROXY_DNS_NAME, PROXY_PORT, 1);

    Rows resultBefore =
        (Rows)
            interceptQuery(interceptor, "SELECT * FROM system.peers", PUBLIC_SOURCE, PUBLIC_PEER1);
    assertThat(collect(resultBefore, "rpc_address"))
        .containsExactlyInAnyOrder(PUBLIC_PEER2, PUBLIC_PEER3);

    await()
        .atMost(Duration.ofSeconds(3))
        .until(
            () -> {
              Rows resultAfter =
                  (Rows)
                      interceptQuery(
                          interceptor, "SELECT * FROM system.peers", PUBLIC_SOURCE, PUBLIC_PEER1);
              return collect(resultAfter, "rpc_address").equals(ImmutableSet.of(PUBLIC_PEER2));
            });
  }

  private <T> Set<T> collect(Rows result, String name) {
    return result.rows.stream()
        .map(row -> (T) columnValue(row, result.resultMetadata, name))
        .collect(Collectors.toSet());
  }

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
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

  private QueryState newQueryState(InetSocketAddress source, InetAddress destination) {
    return new QueryState(
        StargateClientState.forExternalCalls(
            new ClientInfo(source, new InetSocketAddress(destination, 9042))),
        UserRolesAndPermissions.ANONYMOUS);
  }

  private Result interceptQuery(
      ProxyProtocolQueryInterceptor interceptor,
      String query,
      InetSocketAddress source,
      InetAddress destination) {
    QueryState queryState = newQueryState(source, destination);
    CQLStatement statement = QueryProcessor.parseStatement(query, queryState);
    interceptor.initialize();
    return Conversion.toResult(
        interceptor.interceptQuery(statement, queryState, null, null, 0).blockingGet(),
        ProtocolVersion.V4,
        null);
  }

  private static InetAddress getRawAddress(int... parts) {
    byte[] byteParts = new byte[parts.length];
    for (int i = 0; i < parts.length; i++) {
      byteParts[i] = (byte) parts[i];
    }
    try {
      return InetAddress.getByAddress(null, byteParts);
    } catch (UnknownHostException e) {
      return fail("This shouldn't happen since we provided a raw address", e);
    }
  }
}
