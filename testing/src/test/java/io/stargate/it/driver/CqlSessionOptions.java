package io.stargate.it.driver;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import java.time.Duration;
import javax.validation.constraints.NotNull;

public class CqlSessionOptions {
  @NotNull
  public static OptionsMap defaultConfig() {
    OptionsMap config;
    config = OptionsMap.driverDefaults();
    config.put(TypedDriverOption.METADATA_TOKEN_MAP_ENABLED, false);
    config.put(
        TypedDriverOption.LOAD_BALANCING_POLICY_CLASS,
        DcInferringLoadBalancingPolicy.class.getName());
    config.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(180));
    config.put(TypedDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofSeconds(180));
    config.put(TypedDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(180));
    config.put(TypedDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(180));
    config.put(TypedDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(5));
    config.put(TypedDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false);
    return config;
  }
}
