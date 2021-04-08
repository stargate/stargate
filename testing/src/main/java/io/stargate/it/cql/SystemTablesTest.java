package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.collect.Streams;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.it.storage.StargateSpec;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(nodes = 2)
@ExtendWith(CqlSessionExtension.class)
class SystemTablesTest extends BaseOsgiIntegrationTest {
  @Test
  @DisplayName("Should expose Stargate addresses in system.local and system.peers")
  public void querySystemLocalAndPeers(CqlSession session) {
    Iterator<Node> nodes = session.getMetadata().getNodes().values().iterator();

    Node localNode = nodes.next();
    Row localRow =
        session
            .execute(
                SimpleStatement.builder("SELECT * FROM system.local").setNode(localNode).build())
            .one();
    assertThat(localRow).isNotNull();
    assertThat(localRow.getInetAddress("listen_address")).isEqualTo(getNodeAddress(localNode));
    assertThat(localRow.getSet("tokens", String.class)).hasSizeGreaterThan(1);

    ResultSet rs =
        session.execute(
            SimpleStatement.builder("SELECT * FROM system.peers").setNode(localNode).build());
    List<InetAddress> peersAddresses = new ArrayList<>();
    rs.forEach(
        row -> {
          peersAddresses.add(row.getInetAddress("peer"));
          assertThat(row.getSet("tokens", String.class)).hasSizeGreaterThan(1);
        });
    List<InetAddress> expectedPeersAddresses =
        Streams.stream(nodes).map(n -> getNodeAddress(n)).collect(Collectors.toList());
    assertThat(peersAddresses).containsExactlyInAnyOrderElementsOf(expectedPeersAddresses);
  }

  @Test
  @DisplayName("Should add/remove Stargate addresses from system.peers")
  public void addAndRemovePeers(CqlSession session, StargateEnvironmentInfo environmentInfo)
      throws Exception {
    Node localNode = session.getMetadata().getNodes().values().iterator().next();

    StargateConnectionInfo newNode = environmentInfo.addNode();
    await()
        .atMost(Duration.ofMinutes(5))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> session.getMetadata().getNodes().size() > 2);

    InetAddress newNodeAddress = InetAddress.getByName(newNode.seedAddress());

    Row newNodeRow = queryPeer(session, localNode, newNodeAddress);
    assertThat(newNodeRow).isNotNull();
    assertThat(newNodeRow.getInetAddress("peer")).isEqualTo(newNodeAddress);

    environmentInfo.removeNode(newNode);
    await()
        .atMost(Duration.ofMinutes(5))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> session.getMetadata().getNodes().size() < 3);

    Row removedNodeRow = queryPeer(session, localNode, newNodeAddress);
    assertThat(removedNodeRow).isNull();
  }

  private static Row queryPeer(CqlSession session, Node localNode, InetAddress peer) {
    return session
        .execute(
            SimpleStatement.builder("SELECT * FROM system.peers WHERE peer = ?")
                .setNode(localNode)
                .addPositionalValue(peer)
                .build())
        .one();
  }

  private static InetAddress getNodeAddress(Node node) {
    return node.getListenAddress()
        .map(l -> l.getAddress())
        .orElse(node.getBroadcastRpcAddress().map(b -> b.getAddress()).orElse(null));
  }
}
