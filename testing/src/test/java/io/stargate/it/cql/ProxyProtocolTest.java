package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@SkipWhenNotDse
@SkipIfProxyDnsInvalid
@StargateSpec(parametersCustomizer = "buildParameters")
@CqlSessionSpec(
    contactPointResolver = ProxyContactPointResolver.class,
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k uuid PRIMARY KEY, v int)",
    })
@ProxySpec(numProxies = 2)
@ExtendWith({ProxyExtension.class, CqlSessionExtension.class})
public class ProxyProtocolTest extends BaseOsgiIntegrationTest {
  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.useProxyProtocol(true);
    builder.putSystemProperties("stargate.cql_use_auth_service", "true");
  }

  @Test
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
  public void queryDistribution(CqlSession session) {
    final int numQueries = 100;
    PreparedStatement preparedStatement = session.prepare("INSERT INTO test (k, v) VALUES (?, ?)");

    class Counter {
      public int count = 0;

      void increment() {
        ++count;
      }
    }

    Map<Node, Counter> counts = new HashMap<>();
    for (int i = 0; i < numQueries; ++i) {
      ResultSet rs = session.execute(preparedStatement.bind(UUID.randomUUID(), i));
      counts
          .computeIfAbsent(rs.getExecutionInfo().getCoordinator(), n -> new Counter())
          .increment();
    }

    final int expectedCount = numQueries / counts.size();
    for (Map.Entry<Node, Counter> entry : counts.entrySet()) {
      assertThat(entry.getValue().count).isCloseTo(expectedCount, Percentage.withPercentage(25));
    }
  }
}
