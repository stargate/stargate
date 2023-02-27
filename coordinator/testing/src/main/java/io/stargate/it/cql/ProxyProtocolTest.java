package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.proxy.ProxyAddresses;
import io.stargate.it.proxy.ProxyContactPointResolver;
import io.stargate.it.proxy.ProxyExtension;
import io.stargate.it.proxy.ProxySpec;
import io.stargate.it.proxy.SkipIfProxyDnsInvalid;
import io.stargate.it.storage.SkipWhenNotDse;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@SkipWhenNotDse
@SkipIfProxyDnsInvalid
@StargateSpec(parametersCustomizer = "buildParameters")
@CqlSessionSpec(contactPointResolver = ProxyContactPointResolver.class)
@ProxySpec(numProxies = 2)
@ExtendWith({ProxyExtension.class, CqlSessionExtension.class})
public class ProxyProtocolTest extends BaseIntegrationTest {
  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.useProxyProtocol(true);
    builder.putSystemProperties("stargate.cql_use_auth_service", "true");
  }

  @Test
  @DisplayName("Should expose proxy addresses in system.local and system.peers")
  public void querySystemLocalAndPeers(
      CqlSession session, @ProxyAddresses List<InetSocketAddress> proxyAddresses) {
    for (InetSocketAddress proxyAddress : proxyAddresses) {
      Optional<Node> node = session.getMetadata().findNode(proxyAddress);
      assertThat(node).isPresent();

      Row localRow =
          session
              .execute(
                  SimpleStatement.builder("SELECT * FROM system.local").setNode(node.get()).build())
              .one();
      assertThat(localRow).isNotNull();
      assertThat(localRow.getInetAddress("listen_address")).isEqualTo(proxyAddress.getAddress());
      assertThat(localRow.getSet("tokens", String.class)).hasSizeGreaterThan(1);

      ResultSet rs =
          session.execute(
              SimpleStatement.builder("SELECT * FROM system.peers").setNode(node.get()).build());
      List<InetAddress> peersAddresses = new ArrayList<>();
      rs.forEach(
          row -> {
            peersAddresses.add(row.getInetAddress("peer"));
            assertThat(row.getSet("tokens", String.class)).hasSizeGreaterThan(1);
          });
      List<InetAddress> expectedPeersAddresses =
          proxyAddresses.stream()
              .filter(a -> !a.getAddress().equals(proxyAddress.getAddress()))
              .map(a -> a.getAddress())
              .collect(Collectors.toList());
      assertThat(peersAddresses).containsExactlyInAnyOrderElementsOf(expectedPeersAddresses);
    }
  }

  @Test
  @DisplayName("Should use all proxy addresses when using token-aware load balancing")
  public void testTokenMapDistribution(
      CqlSession session,
      @ProxyAddresses List<InetSocketAddress> proxyAddresses,
      @TestKeyspace CqlIdentifier keyspace) {
    assertThat(session.getMetadata().getTokenMap()).isPresent();
    final TokenMap tokenMap = session.getMetadata().getTokenMap().get();

    Collection<Node> expectedReplicasTried = session.getMetadata().getNodes().values();
    assertThat(proxyAddresses).hasSize(expectedReplicasTried.size());

    Set<Node> allReplicasTried = new HashSet<>();
    for (int i = 0; i < 10 * expectedReplicasTried.size(); ++i) {
      Set<Node> replicas =
          tokenMap.getReplicas(
              keyspace, TypeCodecs.UUID.encode(UUID.randomUUID(), ProtocolVersion.DEFAULT));
      assertThat(replicas).isNotEmpty();
      Node replica = replicas.iterator().next();
      allReplicasTried.add(replica);
    }

    assertThat(allReplicasTried).containsExactlyInAnyOrderElementsOf(expectedReplicasTried);
  }
}
