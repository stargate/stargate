package io.stargate.cql;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.auth.AuthenticationService;
import io.stargate.cql.impl.CqlImpl;
import io.stargate.db.Persistence;
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import org.apache.cassandra.stargate.config.Config;

public class CqlConfiguration {

  private static final ObjectMapper mapper =
      new ObjectMapper(new YAMLFactory())
          .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);

  private static final boolean USE_AUTH_SERVICE =
      Boolean.parseBoolean(System.getProperty("stargate.cql_use_auth_service", "false"));

  @Produces
  @ApplicationScoped
  public CqlImpl cqlImpl(
      Persistence persistence,
      MeterRegistry meterRegistry,
      Instance<AuthenticationService> authenticationService,
      ClientInfoMetricsTagProvider clientInfoMetricsTagProvider) {
    if (USE_AUTH_SERVICE && authenticationService.isResolvable()) {
      return new CqlImpl(
          makeConfig(),
          persistence,
          meterRegistry,
          authenticationService.get(),
          clientInfoMetricsTagProvider);
    } else {
      return new CqlImpl(
          makeConfig(), persistence, meterRegistry, null, clientInfoMetricsTagProvider);
    }
  }

  private static Config makeConfig() {
    try {
      Config c;

      String cqlConfigPath = System.getProperty("stargate.cql.config_path", "");
      if (cqlConfigPath.isEmpty()) {
        c = new Config();
      } else {
        File configFile = new File(cqlConfigPath);
        c = mapper.readValue(configFile, Config.class);
      }

      String listenAddress =
          System.getProperty(
              "stargate.listen_address", InetAddress.getLocalHost().getHostAddress());

      if (!Boolean.getBoolean("stargate.bind_to_listen_address")) listenAddress = "0.0.0.0";

      Integer cqlPort = Integer.getInteger("stargate.cql_port", 9042);

      c.rpc_address = listenAddress;
      c.native_transport_port = cqlPort;

      c.native_transport_max_concurrent_connections =
          Long.getLong("stargate.cql.native_transport_max_concurrent_connections", -1);
      c.native_transport_max_concurrent_connections_per_ip =
          Long.getLong("stargate.cql.native_transport_max_concurrent_connections_per_ip", -1);
      c.native_transport_max_concurrent_requests_in_bytes =
          Long.getLong(
              "stargate.cql.native_transport_max_concurrent_requests_in_bytes",
              Runtime.getRuntime().maxMemory() / 10);
      c.native_transport_max_concurrent_requests_in_bytes_per_ip =
          Long.getLong(
              "stargate.cql.native_transport_max_concurrent_requests_in_bytes_per_ip",
              Runtime.getRuntime().maxMemory() / 40);
      c.native_transport_flush_in_batches_legacy =
          Boolean.getBoolean("stargate.cql.native_transport_flush_in_batches_legacy");
      return c;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
