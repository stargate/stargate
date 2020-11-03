package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.proxy.ProxyAddresses;
import io.stargate.it.proxy.ProxyExtension;
import io.stargate.it.proxy.ProxySpec;
import io.stargate.it.proxy.SkipIfProxyDnsInvalid;
import io.stargate.it.storage.SkipWhenNotDse;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@SkipWhenNotDse
@SkipIfProxyDnsInvalid
@StargateSpec(parametersCustomizer = "buildParameters")
@CqlSessionSpec(createSession = false, noDefaultContactPoints = true)
@ProxySpec(numProxies = 2)
@ExtendWith({CqlSessionExtension.class, ProxyExtension.class})
public class ProxyProtocolTest extends BaseOsgiIntegrationTest {
  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.useProxyProtocol(true);
    builder.putSystemProperties("stargate.cql_use_auth_service", "true");
  }

  @Test
  public void querySystemLocalAndPeers(
      CqlSessionBuilder sessionBuilder, @ProxyAddresses List<InetSocketAddress> proxyAddresses) {
    try (CqlSession proxySession = sessionBuilder.addContactPoints(proxyAddresses).build()) {
      for (InetSocketAddress proxyAddress : proxyAddresses) {
        Optional<Node> node = proxySession.getMetadata().findNode(proxyAddress);
        assertThat(node).isPresent();

        Row localRow =
            proxySession
                .execute(
                    SimpleStatement.builder("SELECT * FROM system.local")
                        .setNode(node.get())
                        .build())
                .one();
        assertThat(localRow).isNotNull();
        assertThat(localRow.getInetAddress("listen_address")).isEqualTo(proxyAddress.getAddress());

        ResultSet rs =
            proxySession.execute(
                SimpleStatement.builder("SELECT * FROM system.peers").setNode(node.get()).build());
        List<InetAddress> peersAddresses = new ArrayList<>();
        rs.forEach(row -> peersAddresses.add(row.getInetAddress("peer")));
        List<InetAddress> expectedPeersAddresses =
            proxyAddresses.stream()
                .filter(a -> !a.getAddress().equals(proxyAddress.getAddress()))
                .map(a -> a.getAddress())
                .collect(Collectors.toList());
        assertThat(peersAddresses).containsExactlyInAnyOrderElementsOf(expectedPeersAddresses);
      }
    }
  }
}
