package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.it.storage.StargateSpec;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(nodes = 3)
@ExtendWith(CqlSessionExtension.class)
public class TokenAwareTest extends BaseIntegrationTest {
  @Test
  @DisplayName("Should use all Stargate addresses when using token-aware load balancing")
  public void testTokenMapDistribution(
      CqlSession session, StargateEnvironmentInfo stargate, @TestKeyspace CqlIdentifier keyspace) {
    assertThat(session.getMetadata().getTokenMap()).isPresent();
    final TokenMap tokenMap = session.getMetadata().getTokenMap().get();

    Collection<Node> expectedReplicasTried = session.getMetadata().getNodes().values();
    assertThat(stargate.nodes()).hasSize(expectedReplicasTried.size());

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
