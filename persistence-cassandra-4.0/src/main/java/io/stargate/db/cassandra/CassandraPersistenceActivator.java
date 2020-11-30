package io.stargate.db.cassandra;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.auth.AuthorizationService;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import io.stargate.db.cassandra.impl.CassandraPersistence;
import io.stargate.db.cassandra.impl.StargateConfigSnitch;
import io.stargate.db.cassandra.impl.StargateSeedProvider;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraPersistenceActivator extends BaseActivator {

  private static final Logger logger = LoggerFactory.getLogger(CassandraPersistenceActivator.class);
  private final ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private final ServicePointer<AuthorizationService> authorizationService =
      ServicePointer.create(
          AuthorizationService.class,
          "AuthIdentifier",
          System.getProperty("stargate.auth_id", "AuthTableBasedService"));

  public CassandraPersistenceActivator() {
    super("persistence-cassandra-4.0");
  }

  @VisibleForTesting
  public static Config makeConfig(File baseDir) throws IOException {
    Config c = new Config();

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
      c.authenticator = PasswordAuthenticator.class.getCanonicalName();
      c.authorizer = CassandraAuthorizer.class.getCanonicalName();
    }

    c.dynamic_snitch = Boolean.getBoolean("stargate.dynamic_snitch");
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
    c.broadcast_address = System.getProperty("stargate.broadcast_address", listenAddress);
    c.seed_provider =
        new ParameterizedClass(
            StargateSeedProvider.class.getName(), Collections.singletonMap("seeds", seedList));

    return c;
  }

  @Override
  protected ServiceAndProperties createService() {
    CassandraPersistence cassandraDB = new CassandraPersistence();
    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", "CassandraPersistence");
    // TODO copy metrics if this gets invoked more than once?
    CassandraMetricsRegistry.actualRegistry =
        metrics.get().getRegistry("persistence-cassandra-4.0");

    try {
      // Throw away data directory since stargate is ephemeral anyway
      File baseDir = Files.createTempDirectory("stargate-cassandra-4.0").toFile();

      cassandraDB.setAuthorizationService(new AtomicReference<>(authorizationService.get()));
      cassandraDB.initialize(makeConfig(baseDir));
      return new ServiceAndProperties(cassandraDB, Persistence.class, props);
    } catch (IOException e) {
      logger.error("Error initializing cassandra persistence", e);
      throw new IOError(e);
    }
  }

  @Override
  protected void stopService() {
    // no-op
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(metrics, authorizationService);
  }
}
