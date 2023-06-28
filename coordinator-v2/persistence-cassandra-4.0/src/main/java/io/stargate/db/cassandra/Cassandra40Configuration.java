package io.stargate.db.cassandra;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import io.stargate.auth.AuthorizationProcessor;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.cassandra.impl.Cassandra40Persistence;
import io.stargate.db.cassandra.impl.DelegatingAuthorizer;
import io.stargate.db.cassandra.impl.StargateConfigSnitch;
import io.stargate.db.cassandra.impl.StargateSeedProvider;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

public class Cassandra40Configuration {

  @Singleton
  @Produces
  public MetricRegistry metricRegistry() {
    // TODO should somebody else provide meter registry?
    return new MetricRegistry();
  }

  @Singleton
  @Produces
  public Cassandra40Persistence persistence(
      MetricRegistry metricRegistry,
      Instance<AuthorizationService> authorizationService,
      Instance<AuthorizationProcessor> authorizationProcessor)
      throws Exception {
    // TODO Prefixed metric registry here,
    CassandraMetricsRegistry.actualRegistry = metricRegistry;

    Cassandra40Persistence cassandraDB = new Cassandra40Persistence();
    cassandraDB.setAuthorizationService(authorizationService);
    cassandraDB.initialize(makeConfig(getBaseDir()));

    IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
    if (authorizer instanceof DelegatingAuthorizer && authorizationProcessor.isResolvable()) {
      ((DelegatingAuthorizer) authorizer).setProcessor(authorizationProcessor.get());
    }

    return cassandraDB;
  }

  // TODO: system props to config
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

    // Add hook to cleanup
    FileUtils.deleteRecursiveOnExit(baseDir);

    String clusterName = System.getProperty("stargate.cluster_name", "stargate-cassandra");
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
      c.authenticator =
          System.getProperty(
              "stargate.authenticator_class_name", PasswordAuthenticator.class.getCanonicalName());
      c.authorizer = DelegatingAuthorizer.class.getCanonicalName();
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
    c.seed_provider =
        new ParameterizedClass(
            StargateSeedProvider.class.getName(), Collections.singletonMap("seeds", seedList));
    c.enable_user_defined_functions = Boolean.getBoolean("stargate.enable_user_defined_functions");

    return c;
  }

  /**
   * Creates a throw away data directory for Stargate's ephemeral files. It can be overridden using
   * the system property {@code "stargate.basedir"} which can be useful on systems where the
   * temporary directory is periodically cleaned.
   *
   * @return A file handle to the base directory.
   * @throws IOException if the base directory is invalid or unable to be created.
   */
  // TODO system props to config
  protected File getBaseDir() throws IOException {
    String baseDir = System.getProperty("stargate.basedir");
    if (baseDir == null || baseDir.isEmpty()) {
      return Files.createTempDirectory("stargate-persistence-cassandra-4.0").toFile();
    } else {
      return Files.createDirectories(Paths.get(baseDir, "stargate-persistence-cassandra-4.0"))
          .toFile();
    }
  }
}
