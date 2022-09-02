package io.stargate.sgv2.it;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import io.stargate.sgv2.common.IntegrationTestUtils;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class RestApiV2QCqlEnabledTestBase extends RestApiV2QIntegrationTestBase {
  protected RestApiV2QCqlEnabledTestBase(String keyspacePrefix, String tablePrefix) {
    super(keyspacePrefix, tablePrefix);
  }

  protected CqlSession session;

  @BeforeAll
  public final void buildSession() {
    OptionsMap config = OptionsMap.driverDefaults();
    config.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10));
    config.put(TypedDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false);
    config.put(
        TypedDriverOption.LOAD_BALANCING_POLICY_CLASS,
        DcInferringLoadBalancingPolicy.class.getName());
    session =
        CqlSession.builder()
            .addContactPoint(IntegrationTestUtils.getCassandraCqlAddress())
            .withConfigLoader(DriverConfigLoader.fromMap(config))
            .build();
  }

  @AfterAll
  public final void closeSession() {
    if (session != null) {
      session.close();
    }
  }
}
