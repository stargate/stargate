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

import static io.stargate.producer.kafka.configuration.DefaultConfigLoader.CONFIG_STORE_MODULE_NAME;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.cdc.CDCProducer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Logic for registering the kafka producer as an OSGI bundle */
public class KafkaProducerActivator extends BaseActivator {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerActivator.class);
  public static final String ENABLED_SETTING_NAME = "enabled";
  public static final String KAFKA_CDC_METRICS_PREFIX = "cdc.kafka";
  public static final boolean IS_ENABLED_DEFAULT = false;

  private ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private ServicePointer<ConfigStore> configStore = ServicePointer.create(ConfigStore.class);

  public KafkaProducerActivator() {
    super("Kafka producer", KafkaCDCProducer.class);
  }

  @Nullable
  @Override
  protected ServiceAndProperties createService() {
    boolean isEnabled = isServiceEnabled(configStore.get());
    if (isEnabled) {
      LOG.info("CDC Kafka producer is enabled");
      CDCProducer producer =
          new KafkaCDCProducer(
              metrics.get().getRegistry(KAFKA_CDC_METRICS_PREFIX), configStore.get());
      return new ServiceAndProperties(producer);
    } else {
      LOG.info("CDC Kafka producer is disabled");
      return null;
    }
  }

  @VisibleForTesting
  protected boolean isServiceEnabled(ConfigStore configStore) {
    try {
      return Optional.ofNullable(
              configStore
                  .getConfigForModule(CONFIG_STORE_MODULE_NAME)
                  .getWithOverrides(ENABLED_SETTING_NAME))
          .map(Boolean::parseBoolean)
          .orElse(IS_ENABLED_DEFAULT);
    } catch (Exception exception) {
      LOG.error("isServiceEnabled failed, returning default.", exception);
      return IS_ENABLED_DEFAULT;
    }
  }

  @Override
  protected void stopService() {
    // no-op
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(metrics, configStore);
  }
}
