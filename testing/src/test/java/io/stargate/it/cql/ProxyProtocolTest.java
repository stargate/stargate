package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.proxy.ProxyAddress;
import io.stargate.it.proxy.ProxyExtension;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(parametersCustomizer = "buildParameters")
@CqlSessionSpec(createSession = false, noDefaultContactPoints = true)
@ExtendWith({CqlSessionExtension.class, ProxyExtension.class})
public class ProxyProtocolTest extends BaseOsgiIntegrationTest {
  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.useProxyProtocol(true);
    builder.putSystemProperties("stargate.cql_use_auth_service", "true");
  }

  @Test
  public void querySystemLocalAndPeers(CqlSessionBuilder sessionBuilder, @ProxyAddress
      InetSocketAddress proxyAddress) {
    try (CqlSession proxySession = sessionBuilder.addContactPoint(proxyAddress).build()) {

      Row row = proxySession.execute("SELECT * FROM system.local").one();
      assertThat(row).isNotNull();
      assertThat(proxyAddress.nodes())
          .extracting(StargateConnectionInfo::seedAddress)
          .contains(row.getInetAddress("listen_address").getHostAddress());
    }
  }
}
