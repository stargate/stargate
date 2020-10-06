package io.stargate.it.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Date;
import net.jcip.annotations.NotThreadSafe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for tests that use a dedicated Java driver session (and keyspace) for each method.
 *
 * <p>TODO maybe refactor this into a Junit extension
 */
@NotThreadSafe
public abstract class JavaDriverTestBase extends BaseOsgiIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(JavaDriverTestBase.class);

  protected static final int KEYSPACE_NAME_MAX_LENGTH = 48;

  protected CqlSession session;
  protected CqlIdentifier keyspaceId;

  public JavaDriverTestBase(ClusterConnectionInfo backend) {
    super(backend);
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    session = newSessionBuilder().build();

    keyspaceId = generateKeyspaceId(testInfo);
    LOG.info("Creating keyspace {}", keyspaceId.asCql(true));

    session.execute(
        String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s "
                + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            keyspaceId.asCql(false)));
    session.execute(String.format("USE %s", keyspaceId.asCql(false)));
  }

  protected CqlSessionBuilder newSessionBuilder() {
    OptionsMap config = OptionsMap.driverDefaults();
    config.put(TypedDriverOption.METADATA_TOKEN_MAP_ENABLED, false);
    config.put(
        TypedDriverOption.LOAD_BALANCING_POLICY_CLASS,
        DcInferringLoadBalancingPolicy.class.getName());
    config.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(60));
    config.put(TypedDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(30));
    config.put(TypedDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(5));
    config.put(TypedDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false);
    customizeConfig(config);

    CqlSessionBuilder builder = CqlSession.builder();
    customizeBuilder(builder);
    builder =
        builder
            .withConfigLoader(DriverConfigLoader.fromMap(config))
            .withAuthCredentials("cassandra", "cassandra")
            .addContactPoint(new InetSocketAddress(getStargateHost(), 9043));
    return builder;
  }

  private CqlIdentifier generateKeyspaceId(TestInfo testInfo) {
    return testInfo
        .getTestMethod()
        .map(
            method -> {
              String keyspaceName = "ks_" + new Date().getTime() + "_" + method.getName();
              if (keyspaceName.length() > KEYSPACE_NAME_MAX_LENGTH) {
                keyspaceName = keyspaceName.substring(0, KEYSPACE_NAME_MAX_LENGTH);
              }
              return CqlIdentifier.fromInternal(keyspaceName);
            })
        .orElseThrow(() -> new AssertionError("Could not find test method"));
  }

  @AfterEach
  public void after() {
    if (session != null) {
      LOG.info("Dropping keyspace {}", keyspaceId.asCql(true));
      session.execute(String.format("DROP KEYSPACE IF EXISTS %s", keyspaceId.asCql(false)));
      session.close();
    }
  }

  protected void customizeConfig(OptionsMap config) {
    // nothing by default
  }

  /**
   * Do not invoke {@link SessionBuilder#withAuthCredentials} or {@link
   * SessionBuilder#withConfigLoader} from this method, those calls will be ignored.
   *
   * <p>If you need to customize the config, use {@link #customizeConfig(OptionsMap)}.
   */
  protected void customizeBuilder(CqlSessionBuilder builder) {
    // nothing by default
  }

  // TODO generalize this to an ExecutionCondition that reads custom annotations, like
  // @CassandraRequirement/@DseRequirement in the Java driver tests
  public boolean isCassandra4() {
    return !backend.isDse()
        && Version.parse(backend.clusterVersion()).nextStable().compareTo(Version.V4_0_0) >= 0;
  }
}
