package io.stargate.db.metrics.api;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Produces;

@Dependent
public class ClientInfoMetricsConfiguration {

  @Produces
  @DefaultBean
  public ClientInfoMetricsTagProvider clientInfoMetricsTagProvider() {
    return ClientInfoMetricsTagProvider.DEFAULT;
  }
}
