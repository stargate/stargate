package io.stargate.health;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.server.SimpleServerFactory;

/**
 * This class is made available to DropWizard via {@code
 * META-INF/services/io.dropwizard.server.ServerFactory}, and then enabled via the {@code
 * server.type} option in {@code config.yaml}.
 */
@JsonTypeName("health-checker")
public class HealthCheckerServerFactory extends SimpleServerFactory {}
