package io.stargate.it.cql.protocolV4;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(nodes = 1, shared = false, parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(customOptions = "applyProtocolVersion")
public class HostIdTest extends BaseIntegrationTest {
  private static final String hostId =
      UUID.nameUUIDFromBytes("test123".getBytes(StandardCharsets.UTF_8)).toString();

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.putSystemProperties("stargate.host_id", hostId);
  }

  public static void applyProtocolVersion(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
  }

  @Test
  @DisplayName(
      "Should expose the host ID in system.local set using the property `stargate.host_id`")
  public void queryHostId(CqlSession session) {
    Row localRow =
        session.execute(SimpleStatement.builder("SELECT host_id FROM system.local").build()).one();
    assertThat(localRow).isNotNull();
    UUID localHostId = localRow.getUuid("host_id");
    assertThat(localHostId.toString()).isEqualTo(hostId);
  }
}
