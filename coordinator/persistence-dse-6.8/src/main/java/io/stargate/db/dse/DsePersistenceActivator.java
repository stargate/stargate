package io.stargate.db.dse;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.stargate.auth.AuthorizationProcessor;
import io.stargate.auth.AuthorizationService;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.common.util.UserDefinedFunctionHelper;
import io.stargate.db.dse.impl.DelegatingAuthorizer;
import io.stargate.db.dse.impl.DsePersistence;
import io.stargate.db.dse.impl.StargateConfigSnitch;
import io.stargate.db.dse.impl.StargateSeedProvider;
import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.GuardrailsConfig;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.io.FileUtils;

public class DsePersistenceActivator extends BaseActivator {

  static {
    // 28-Sep-2023, tatu: Fix needed pre C*4.1 (fixed via CASSANDRA-17013
    //   https://issues.apache.org/jira/browse/CASSANDRA-17013).
    //  Problematic with JDK 12+ but possible to force with JDK param:
    //  "-Djdk.reflect.useDirectMethodHandle=false"
    UserDefinedFunctionHelper.fixCompilerClassLoader();
  }

  private static final String AUTHZ_PROCESSOR_ID =
      System.getProperty("stargate.authorization.processor.id");

  private final ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private final LazyServicePointer<AuthorizationService> authorizationService =
      LazyServicePointer.create(
          AuthorizationService.class,
          "AuthIdentifier",
          System.getProperty("stargate.auth_id", "AuthTableBasedService"));
  private final ServicePointer<AuthorizationProcessor> authorizationProcessor =
      ServicePointer.create(AuthorizationProcessor.class, "AuthProcessorId", AUTHZ_PROCESSOR_ID);

  private DsePersistence dseDB;
  private File baseDir;

  public DsePersistenceActivator() {
    super("DSE Stargate Backend");
  }

  @VisibleForTesting
  public static Config makeConfig(File baseDir) throws IOException {
    Config c;

    String cassandraConfigPath = System.getProperty("stargate.unsafe.cassandra_config_path", "");
    if (cassandraConfigPath.isEmpty()) {
      c = new Config();
    } else {
      File configFile = new File(cassandraConfigPath);
      c = new YamlConfigurationLoader().loadConfig(configFile.toURI().toURL());
    }

    File commitLogDir = Paths.get(baseDir.getPath(), "commitlog").toFile();
    commitLogDir.mkdirs();

    File dataDir = Paths.get(baseDir.getPath(), "data").toFile();
    dataDir.mkdirs();

    File hintDir = Paths.get(baseDir.getPath(), "hints").toFile();
    hintDir.mkdirs();

    File cdcDir = Paths.get(baseDir.getPath(), "cdc").toFile();
    cdcDir.mkdirs();

    File cacheDir = Paths.get(baseDir.getPath(), "caches").toFile();
    cacheDir.mkdirs();

    File metadataDir = Paths.get(baseDir.getPath(), "metadata").toFile();
    metadataDir.mkdirs();

    String clusterName = System.getProperty("stargate.cluster_name", "stargate-dse");
    String listenAddress =
        System.getProperty("stargate.listen_address", InetAddress.getLocalHost().getHostAddress());
    String broadcastAddress = System.getProperty("stargate.broadcast_address", listenAddress);
    Integer cqlPort = Integer.getInteger("stargate.cql_port", 9042);
    Integer listenPort = Integer.getInteger("stargate.seed_port", 7000);
    String seedList = System.getProperty("stargate.seed_list", "");
    String snitchClass =
        System.getProperty(
            "stargate.snitch_classname", StargateConfigSnitch.class.getCanonicalName());

    if (snitchClass.equalsIgnoreCase("SimpleSnitch"))
      snitchClass = SimpleSnitch.class.getCanonicalName();

    if (snitchClass.equalsIgnoreCase("StargateConfigSnitch"))
      snitchClass = StargateConfigSnitch.class.getCanonicalName();

    String enableAuth = System.getProperty("stargate.enable_auth", "false");

    if (enableAuth.equalsIgnoreCase("true")) {
      // TODO: Use DseAuthenticator and DseAuthorizer. We need to configure them properly
      c.authenticator =
          System.getProperty(
              "stargate.authenticator_class_name", PasswordAuthenticator.class.getCanonicalName());
      c.authorizer = DelegatingAuthorizer.class.getName();
    }

    c.cluster_name = clusterName;
    c.num_tokens = Integer.getInteger("stargate.num_tokens", 256);
    c.commitlog_sync = Config.CommitLogSync.periodic;
    c.commitlog_sync_period_in_ms = 10000;
    c.internode_compression = Config.InternodeCompression.none;
    c.commitlog_directory = commitLogDir.getAbsolutePath();
    c.hints_directory = hintDir.getAbsolutePath();
    c.cdc_raw_directory = cdcDir.getAbsolutePath();
    c.saved_caches_directory = cacheDir.getAbsolutePath();
    c.metadata_directory = metadataDir.getAbsolutePath();
    c.data_file_directories = new String[] {dataDir.getAbsolutePath()};
    c.partitioner = Murmur3Partitioner.class.getCanonicalName();
    c.disk_failure_policy = Config.DiskFailurePolicy.best_effort;
    c.start_native_transport = false;
    c.native_transport_port = cqlPort;
    c.rpc_address = "0.0.0.0";
    c.broadcast_rpc_address = listenAddress;
    c.endpoint_snitch = snitchClass;
    c.storage_port = listenPort;
    c.listen_address = listenAddress;
    c.broadcast_address = broadcastAddress;
    c.tpc_cores = FBUtilities.getAvailableProcessors();
    c.seed_provider =
        new ParameterizedClass(
            StargateSeedProvider.class.getName(), Collections.singletonMap("seeds", seedList));

    if (Boolean.parseBoolean(System.getProperty("stargate.emulate_dbaas_defaults", "false"))) {
      c.emulate_dbaas_defaults = true;
      GuardrailsConfig guardrailsConfig = new GuardrailsConfig();
      guardrailsConfig.secondary_index_per_table_failure_threshold = 6;
      c.guardrails = guardrailsConfig;
    }

    if (Boolean.getBoolean("stargate.system_keyspaces_filtering")) {
      c.system_keyspaces_filtering = true;
    }

    long timeout;
    if ((timeout = Long.getLong("stargate.request_timeout_in_ms", -1)) > 0) {
      c.request_timeout_in_ms = timeout;
    }

    if ((timeout = Long.getLong("stargate.write_request_timeout_in_ms", -1)) > 0) {
      c.write_request_timeout_in_ms = timeout;
    }

    if ((timeout = Long.getLong("stargate.read_request_timeout_in_ms", -1)) > 0) {
      c.read_request_timeout_in_ms = timeout;
    }
    c.enable_user_defined_functions = Boolean.getBoolean("stargate.enable_user_defined_functions");

    return c;
  }

  @Override
  protected ServiceAndProperties createService() {
    dseDB = new DsePersistence();
    // TODO copy metrics if this gets invoked more than once?
    CassandraMetricsRegistry.actualRegistry = metrics.get().getRegistry("persistence-dse-6.8");
    @SuppressWarnings("JdkObsolete")
    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", "DsePersistence");
    try {
      baseDir = getBaseDir();

      dseDB.setAuthorizationService(authorizationService.get());
      dseDB.initialize(makeConfig(baseDir));

      IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer().implementation();
      if (authorizer instanceof DelegatingAuthorizer) {
        ((DelegatingAuthorizer) authorizer).setProcessor(authorizationProcessor.get());
      }

      return new ServiceAndProperties(dseDB, Persistence.class, props);
    } catch (IOException e) {
      throw new IOError(e);
    }
  }

  @Override
  protected void stopService() {
    try (Closeable ignored = () -> FileUtils.deleteDirectory(baseDir)) {
      if (dseDB != null) {
        dseDB.destroy();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    ImmutableList.Builder<ServicePointer<?>> dependencies = ImmutableList.builder();
    dependencies.add(metrics);

    if (AUTHZ_PROCESSOR_ID != null) {
      dependencies.add(authorizationProcessor);
    }

    return dependencies.build();
  }

  @Override
  protected List<LazyServicePointer<?>> lazyDependencies() {
    return Collections.singletonList(authorizationService);
  }
}
