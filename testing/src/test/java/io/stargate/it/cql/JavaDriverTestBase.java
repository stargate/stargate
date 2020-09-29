package io.stargate.it.cql;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Date;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.ClusterConnectionInfo;
import net.jcip.annotations.NotThreadSafe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Base class for tests that use a dedicated Java driver session (and keyspace) for each method.
 *
 * <p>TODO maybe refactor this into a Junit extension
 */
@NotThreadSafe
public abstract class JavaDriverTestBase extends BaseOsgiIntegrationTest {

  protected CqlSession session;
  protected CqlIdentifier keyspaceId;

  private static final int KEYSPACE_NAME_MAX_LENGTH = 48;

  public JavaDriverTestBase(ClusterConnectionInfo backend) {
    super(backend);
  }

  @BeforeEach
  public void setup(TestInfo testInfo) {
    OptionsMap config = OptionsMap.driverDefaults();
    config.put(TypedDriverOption.METADATA_TOKEN_MAP_ENABLED, false);
    config.put(
        TypedDriverOption.LOAD_BALANCING_POLICY_CLASS,
        DcInferringLoadBalancingPolicy.class.getName());
    config.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5));
    config.put(TypedDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(5));
    config.put(TypedDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(5));
    config.put(TypedDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false);
    customizeConfig(config);

    session =
        CqlSession.builder()
            .withConfigLoader(DriverConfigLoader.fromMap(config))
            .withAuthCredentials("cassandra", "cassandra")
            .addContactPoint(new InetSocketAddress(getStargateHost(), 9043))
            .build();

    String testMethodName =
        testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new);
    String keyspaceName = "ks_" + new Date().getTime() + "_" + testMethodName;
    if (keyspaceName.length() > KEYSPACE_NAME_MAX_LENGTH) {
      keyspaceName = keyspaceName.substring(0, KEYSPACE_NAME_MAX_LENGTH);
    }
    keyspaceId = CqlIdentifier.fromInternal(keyspaceName);

    session.execute(
        String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s "
                + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            keyspaceId.asCql(false)));
    session.execute(String.format("USE %s", keyspaceId.asCql(false)));
  }

  @AfterEach
  public void after() {
    if (session != null) {
      session.execute(String.format("DROP KEYSPACE IF EXISTS %s", keyspaceId.asCql(false)));
      session.close();
    }
  }

  protected void customizeConfig(OptionsMap config) {
    // nothing by default
  }
}
