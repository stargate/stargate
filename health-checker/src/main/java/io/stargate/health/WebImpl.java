package io.stargate.health;

import io.stargate.core.metrics.api.Metrics;
import org.osgi.framework.BundleContext;

public class WebImpl {
  private BundleContext context;
  private Metrics metrics;

  public WebImpl(BundleContext context, Metrics metrics) {
    this.context = context;
    this.metrics = metrics;
  }

  public void start() throws Exception {
    Server server = new Server(new BundleService(context), metrics);
    server.run("server", "config.yaml");
  }
}
