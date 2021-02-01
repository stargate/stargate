package io.stargate.health;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BundleService {
  private static final Logger logger = LoggerFactory.getLogger(BundleService.class);

  private static final String HEALTH_NAME_HEADER = "x-Stargate-Health-Checkers";

  private final BundleContext context;

  public BundleService(BundleContext context) {
    this.context = context;
  }

  public Set<String> defaultHealthCheckNames() {
    Set<String> result = new HashSet<>();
    for (Bundle bundle : context.getBundles()) {
      String header = bundle.getHeaders().get(HEALTH_NAME_HEADER);
      if (header != null) {
        result.addAll(Arrays.asList(header.split(",")));
      }
    }

    return result;
  }
}
