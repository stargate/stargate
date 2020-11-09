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
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.cdc.CDCProducer;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/* Logic for registering the kafka producer as an OSGI bundle */
public class KafkaProducerActivator extends BaseActivator {
  public static final String KAFKA_CDC_METRICS_PREFIX = "cdc.kafka";

  private ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private ServicePointer<ConfigStore> configStore = ServicePointer.create(ConfigStore.class);

  public KafkaProducerActivator() {
    super("Kafka producer", KafkaCDCProducer.class);
  }

  @Nullable
  @Override
  protected ServiceAndProperties createService() {
    CDCProducer producer =
        new KafkaCDCProducer(
            metrics.get().getRegistry(KAFKA_CDC_METRICS_PREFIX), configStore.get());
    return new ServiceAndProperties(producer);
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
