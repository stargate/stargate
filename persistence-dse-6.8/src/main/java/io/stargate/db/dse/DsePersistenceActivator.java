package io.stargate.db.dse;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.auth.AuthorizationService;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.common.StargateConfigSnitch;
import io.stargate.db.datastore.common.StargateSeedProvider;
import io.stargate.db.dse.impl.DsePersistence;
import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.GuardrailsConfig;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.io.FileUtils;

public class DsePersistenceActivator extends BaseActivator {

  private final ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private final LazyServicePointer<AuthorizationService> authorizationService =
      LazyServicePointer.create(
          AuthorizationService.class,
          "AuthIdentifier",
          System.getProperty("stargate.auth_id", "AuthTableBasedService"));

  private DsePersistence dseDB;
  private File baseDir;

  public DsePersistenceActivator() {
    super("DSE Stargate Backend");
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

    File metadataDir = Paths.get(baseDir.getPath(), "metadata").toFile();
    metadataDir.mkdirs();

    String clusterName = System.getProperty("stargate.cluster_name", "stargate-dse");
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
      // TODO: Use DseAuthenticator and DseAuthorizer. We need to configure them properly
      c.authenticator = PasswordAuthenticator.class.getCanonicalName();
      c.authorizer = CassandraAuthorizer.class.getCanonicalName();
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
    c.broadcast_address = listenAddress;
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

    return c;
  }

  @Override
  protected ServiceAndProperties createService() {
    dseDB = new DsePersistence();
    // TODO copy metrics if this gets invoked more than once?
    CassandraMetricsRegistry.actualRegistry = metrics.get().getRegistry("persistence-dse-68");
    @SuppressWarnings("JdkObsolete")
    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", "DsePersistence");
    try {
      // Throw away data directory since stargate is ephemeral anyway
      baseDir = Files.createTempDirectory("stargate-dse").toFile();

      dseDB.setAuthorizationService(authorizationService.get());
      dseDB.initialize(makeConfig(baseDir));
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
    return Collections.singletonList(metrics);
  }

  @Override
  protected List<LazyServicePointer<?>> lazyDependencies() {
    return Collections.singletonList(authorizationService);
  }
}
