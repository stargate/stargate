/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.health;

import com.codahale.metrics.health.HealthCheck;
import java.util.ArrayList;
import java.util.List;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BundleStateChecker extends HealthCheck {
  private static final Logger logger = LoggerFactory.getLogger(BundleStateChecker.class);

  private final BundleContext context;

  public BundleStateChecker(BundleContext context) {
    this.context = context;
  }

  @Override
  protected Result check() {
    List<String> inactive = new ArrayList<>();

    for (Bundle bundle : context.getBundles()) {
      if (bundle.getState() != Bundle.ACTIVE) {
        inactive.add(bundle.getSymbolicName());
      }
    }

    if (inactive.isEmpty()) {
      return Result.healthy("All bundles active");
    } else {
      logger.warn("Inactive bundles: {}", inactive);
      return Result.unhealthy("Inactive bundles: " + inactive);
    }
  }
}
