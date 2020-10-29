/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.producer.kafka;

import io.stargate.config.store.api.ConfigStore;
import io.stargate.core.BundleUtils;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.cdc.CDCProducer;
import javax.annotation.concurrent.GuardedBy;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Logic for registering the kafka producer as an OSGI bundle */
public class KafkaProducerActivator implements BundleActivator, ServiceListener {
  private static final String KAFKA_CDC_METRICS_PREFIX = "cdc.kafka";

  private static final Logger logger = LoggerFactory.getLogger(KafkaProducerActivator.class);

  boolean started;

  private BundleContext context;

  @GuardedBy("this")
  private Metrics metrics;

  @GuardedBy("this")
  private ConfigStore configStore;

  @Override
  public synchronized void start(BundleContext context) throws InvalidSyntaxException {
    logger.info("Registering Kafka producer...");
    this.context = context;

    ServiceReference<?> metricsReference = context.getServiceReference(Metrics.class.getName());
    if (metricsReference == null) {
      logger.debug(
          "Metrics service is null, registering a listener to get notification when it will be ready.");
      context.addServiceListener(this, String.format("(objectClass=%s)", Metrics.class.getName()));
    }

    ServiceReference<?> configStoreReference =
        context.getServiceReference(ConfigStore.class.getName());
    if (configStoreReference == null) {
      logger.debug(
          "Config Store is null, registering a listener to get notification when it will be ready.");
      context.addServiceListener(
          this, String.format("(objectClass=%s)", ConfigStore.class.getName()));
    }

    if (metricsReference == null || configStoreReference == null) {
      // It will be started once the metrics service AND config-store is registered
      return;
    }

    Metrics metrics = (Metrics) context.getService(metricsReference);
    ConfigStore configStore = (ConfigStore) context.getService(configStoreReference);
    startKafkaCDCProducer(metrics, configStore);
  }

  @Override
  public synchronized void stop(BundleContext context) {}

  @Override
  public synchronized void serviceChanged(ServiceEvent serviceEvent) {
    Metrics newMetrics = BundleUtils.getRegisteredService(context, serviceEvent, Metrics.class);
    // capture registration only if the instance is not null
    if (newMetrics != null) {
      metrics = newMetrics;
    }
    ConfigStore newConfigStore =
        BundleUtils.getRegisteredService(context, serviceEvent, ConfigStore.class);
    // capture registration only if the instance is not null
    if (newConfigStore != null) {
      configStore = newConfigStore;
    }

    // if both registered successfully, start the producer
    if (metrics != null && configStore != null) {
      startKafkaCDCProducer(metrics, configStore);
    }
  }

  private synchronized void startKafkaCDCProducer(Metrics metrics, ConfigStore configStore) {
    if (started) {
      logger.info("The Kafka producer is already started. Ignoring the start request.");
      return;
    }

    started = true;
    logger.info("Starting Kafka producer....");
    try {
      CDCProducer producer =
          new KafkaCDCProducer(metrics.getRegistry(KAFKA_CDC_METRICS_PREFIX), configStore);
      context.registerService(CDCProducer.class, producer, null);
      logger.info("Started Kafka producer....");
    } catch (Exception e) {
      logger.error("Failed while starting Kafka producer", e);
    }
  }
}
