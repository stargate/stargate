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
package io.stargate.config.store.yaml;

import io.stargate.config.store.api.ConfigStore;
import io.stargate.core.BundleUtils;
import io.stargate.core.metrics.api.Metrics;
import java.nio.file.Paths;
import java.util.Hashtable;
import javax.annotation.concurrent.GuardedBy;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigStoreActivator implements BundleActivator, ServiceListener {
  private static final Logger logger = LoggerFactory.getLogger(ConfigStoreActivator.class);

  public static final String CONFIG_STORE_YAML_METRICS_PREFIX = "config.store.yaml";
  public static final String CONFIG_STORE_YAML_IDENTIFIER = "ConfigStoreYaml";

  private final String configYamlLocation;

  private BundleContext context;

  @GuardedBy("this")
  boolean started;

  // for testing purpose
  public ConfigStoreActivator(String configYamlLocation) {
    this.configYamlLocation = configYamlLocation;
  }

  public ConfigStoreActivator() {
    this(
        System.getProperty(
            "stargate.config_store.yaml.location", "/etc/stargate/stargate-config.yaml"));
  }

  @Override
  public synchronized void start(BundleContext context) throws InvalidSyntaxException {
    logger.info("Starting Config Store YAML for config file location: {} ...", configYamlLocation);
    this.context = context;

    ServiceReference<?> metricsReference = context.getServiceReference(Metrics.class.getName());
    if (metricsReference == null) {
      logger.debug(
          "Metrics service is null, registering a listener to get notification when it will be ready.");
      context.addServiceListener(this, String.format("(objectClass=%s)", Metrics.class.getName()));
      return;
    }

    Metrics metrics = (Metrics) context.getService(metricsReference);
    startConfigStore(metrics);
  }

  private synchronized void startConfigStore(Metrics metrics) {
    if (started) {
      logger.info("The Config Store YAML is already started. Ignoring the start request.");
      return;
    }
    started = true;

    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", CONFIG_STORE_YAML_IDENTIFIER);

    ConfigStoreYaml configStoreYaml =
        new ConfigStoreYaml(
            Paths.get(configYamlLocation), metrics.getRegistry(CONFIG_STORE_YAML_METRICS_PREFIX));
    context.registerService(ConfigStore.class, configStoreYaml, props);
    logger.info("Started Config Store YAML....");
  }

  @Override
  public void stop(BundleContext context) {
    // no-op
  }

  @Override
  public synchronized void serviceChanged(ServiceEvent event) {
    Metrics metrics = BundleUtils.getRegisteredService(context, event, Metrics.class);
    if (metrics != null) {
      startConfigStore(metrics);
    }
  }
}
