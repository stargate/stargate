package io.stargate.health;

import io.stargate.health.metrics.api.Metrics;
import org.osgi.framework.BundleContext;

public class WebImpl {
  private BundleContext context;
  private Metrics metrics;

  public WebImpl() {}

  public BundleContext getContext() {
    return context;
  }

  public void setContext(BundleContext context) {
    this.context = context;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public void setMetrics(Metrics metrics) {
    this.metrics = metrics;
  }

  public void start() throws Exception {
    Server server = new Server(new BundleService(context), metrics);
    server.run("server", "config.yaml");
  }
}
