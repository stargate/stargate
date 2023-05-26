package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class SupportedOptionsTest extends BaseIntegrationTest {
  @Test
  @DisplayName("Should contain expected supported options")
  public void supportedOptionsTest(CqlSession session) {
    // This only validates the presence of options keys because validating the value depends
    // on the specific backend and could make this test brittle.
    Map<String, List<String>> options = getSupportedOptions(session);
    assertThat(options).containsKey("CQL_VERSION");
    assertThat(options).containsKey("PROTOCOL_VERSIONS");
    assertThat(options).containsKey("COMPRESSION");
    if (backend.isDse()) {
      assertThat(options).containsKey("PAGE_UNIT");
      assertThat(options).containsKey("EMULATE_DBAAS_DEFAULTS");
    }
  }

  private static Map<String, List<String>> getSupportedOptions(CqlSession session) {
    DriverChannel controlChannel =
        ((InternalDriverContext) session.getContext()).getControlConnection().channel();
    assertThat(controlChannel).as("Expected control connection to be connected").isNotNull();
    return controlChannel.getOptions();
  }
}
