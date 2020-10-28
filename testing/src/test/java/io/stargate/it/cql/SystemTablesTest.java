package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.collect.Streams;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateSpec;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@StargateSpec(nodes = 2)
public class SystemTablesTest extends JavaDriverTestBase {
  @Test
  @DisplayName("Should expose Stargate addresses in system.local and system.peers")
  public void querySystemLocalAndPeers() {
    Iterator<Node> nodes = session.getMetadata().getNodes().values().iterator();

    Node localNode = nodes.next();
    Row localRow =
        session
            .execute(
                SimpleStatement.builder("SELECT * FROM system.local").setNode(localNode).build())
            .one();
    assertThat(localRow).isNotNull();
    assertThat(localRow.getInetAddress("rpc_address"))
        .isEqualTo(localNode.getBroadcastRpcAddress().map(s -> s.getAddress()).orElse(null));

    ResultSet rs =
        session.execute(
            SimpleStatement.builder("SELECT * FROM system.peers").setNode(localNode).build());
    List<InetAddress> peersAddresses = new ArrayList<>();
    for (Row row : rs) {
      peersAddresses.add(row.getInetAddress("rpc_address"));
    }
    List<InetAddress> expectedPeersAddresses =
        Streams.stream(nodes)
            .map(n -> n.getBroadcastRpcAddress().map(s -> s.getAddress()).orElse(null))
            .collect(Collectors.toList());
    assertThat(peersAddresses).containsExactlyInAnyOrderElementsOf(expectedPeersAddresses);
  }

  @Test
  @DisplayName("Should add/remove Stagate addresses from system.peers")
  public void addAndRemovePeers() throws Exception {
    Node localNode = session.getMetadata().getNodes().values().iterator().next();

    StargateConnectionInfo newNode = stargateEnvironment.addNode();
    await()
        .atMost(Duration.ofMinutes(5))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> session.getMetadata().getNodes().size() > 2);

    InetAddress newNodeAddress = InetAddress.getByName(newNode.seedAddress());

    Row newNodeRow = queryPeer(localNode, newNodeAddress);
    assertThat(newNodeRow).isNotNull();
    assertThat(newNodeRow.getInetAddress("rpc_address")).isEqualTo(newNodeAddress);

    stargateEnvironment.removeNode(newNode);
    await()
        .atMost(Duration.ofMinutes(5))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> session.getMetadata().getNodes().size() < 3);

    Row removedNodeRow = queryPeer(localNode, newNodeAddress);
    assertThat(removedNodeRow).isNull();
  }

  private Row queryPeer(Node localNode, InetAddress peer) {
    return session
        .execute(
            SimpleStatement.builder("SELECT * FROM system.peers WHERE peer = ?")
                .setNode(localNode)
                .addPositionalValue(peer)
                .build())
        .one();
  }
}
