package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.cql.AdditionalPortsTest.EmptyContactPointResolver;
import io.stargate.it.driver.ContactPointResolver;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
@StargateSpec(nodes = 2, parametersCustomizer = "buildParameters")
@CqlSessionSpec(contactPointResolver = EmptyContactPointResolver.class, createSession = false)
@Order(Integer.MAX_VALUE)
public class AdditionalPortsTest extends BaseIntegrationTest {

  public static final List<Integer> ADDITIONAL_PORTS = Arrays.asList(29042, 39042);

  public static int MAIN_PORT = 9043;

  // The tests are going to set the contact points explicitly
  public static class EmptyContactPointResolver implements ContactPointResolver {

    @Override
    public List<InetSocketAddress> resolve(ExtensionContext context) {
      return Collections.emptyList();
    }
  }

  @BeforeAll
  public static void beforeAll(StargateEnvironmentInfo stargate) {
    assertThat(stargate.nodes()).hasSizeGreaterThan(0);
    MAIN_PORT = stargate.nodes().get(0).cqlPort();
  }

  @ParameterizedTest
  @DisplayName("Connects to stargate using the main and additional port values")
  @MethodSource("allPorts")
  public void connectToMainAndAdditionalPorts(
      int port, CqlSessionBuilder builder, StargateEnvironmentInfo stargate) {
    builder.addContactPoint(new InetSocketAddress(stargate.nodes().get(0).seedAddress(), port));
    CqlSession session = builder.build();

    Iterator<Node> nodes = session.getMetadata().getNodes().values().iterator();

    Node localNode = nodes.next();
    Row localRow =
        session
            .execute(
                SimpleStatement.builder("SELECT * FROM system.local").setNode(localNode).build())
            .one();
    assertThat(localRow).isNotNull();
    assertThat(localRow.getInetAddress("rpc_address"))
        .isEqualTo(
            localNode.getBroadcastRpcAddress().map(InetSocketAddress::getAddress).orElse(null));
    if (backend.isDse()) {
      assertThat(localRow.getInt("native_transport_port")).isEqualTo(port);
    }

    ResultSet rs =
        session.execute(
            SimpleStatement.builder("SELECT * FROM system.peers").setNode(localNode).build());
    List<Row> rows = rs.all();

    assertThat(rows).hasSizeGreaterThan(0);
    List<InetAddress> rpcAddresses = new ArrayList<>();
    rows.forEach(
        row -> {
          rpcAddresses.add(row.getInetAddress("rpc_address"));
          if (backend.isDse()) {
            assertThat(row.getInt("native_transport_port")).isEqualTo(port);
          }
        });
    List<InetAddress> expectedRpcAddresses =
        Streams.stream(nodes)
            .map(n -> n.getBroadcastRpcAddress().map(InetSocketAddress::getAddress).orElse(null))
            .collect(Collectors.toList());
    assertThat(rpcAddresses).containsExactlyInAnyOrderElementsOf(expectedRpcAddresses);
  }

  public static List<Integer> allPorts() {
    return ImmutableList.<Integer>builder().add(MAIN_PORT).addAll(ADDITIONAL_PORTS).build();
  }

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.putSystemProperties(
        "stargate.cql.additional_ports",
        ADDITIONAL_PORTS.stream().map(p -> Integer.toString(p)).collect(Collectors.joining(",")));
  }
}
