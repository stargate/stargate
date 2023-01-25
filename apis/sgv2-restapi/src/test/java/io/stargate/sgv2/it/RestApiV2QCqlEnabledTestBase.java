package io.stargate.sgv2.it;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import io.stargate.sgv2.common.IntegrationTestUtils;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class RestApiV2QCqlEnabledTestBase extends RestApiV2QIntegrationTestBase {
  protected RestApiV2QCqlEnabledTestBase(
      String keyspacePrefix, String tablePrefix, KeyspaceCreation keyspaceCreation) {
    super(keyspacePrefix, tablePrefix, keyspaceCreation);
  }

  protected CqlSession session;

  @BeforeEach
  public final void buildSession() {
    OptionsMap config = OptionsMap.driverDefaults();
    config.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10));
    config.put(TypedDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false);
    config.put(
        TypedDriverOption.LOAD_BALANCING_POLICY_CLASS,
        DcInferringLoadBalancingPolicy.class.getName());

    // resolve auth if enabled
    if (IntegrationTestUtils.isCassandraAuthEnabled()) {
      config.put(TypedDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class.getName());
      config.put(
          TypedDriverOption.AUTH_PROVIDER_USER_NAME, IntegrationTestUtils.getCassandraUsername());
      config.put(
          TypedDriverOption.AUTH_PROVIDER_PASSWORD, IntegrationTestUtils.getCassandraPassword());
    }

    session =
        CqlSession.builder()
            .addContactPoint(IntegrationTestUtils.getCassandraCqlAddress())
            .withConfigLoader(DriverConfigLoader.fromMap(config))
            .build();
  }

  @AfterEach
  public final void closeSession() {
    if (session != null) {
      session.close();
    }
  }
}
