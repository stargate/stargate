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
package io.stargate.producer.kafka.configuration;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.stargate.producer.kafka.mapping.DefaultMappingService;
import io.stargate.producer.kafka.producer.CompletableKafkaProducer;
import io.stargate.producer.kafka.schema.SchemaRegistryProvider;
import java.util.Map;

public class CDCKafkaConfig {
  private final String topicPrefixName;
  private final String schemaRegistryUrl;
  private final Map<String, Object> kafkaProducerSettings;
  private final MetricsConfig metricsConfig;

  public CDCKafkaConfig(
      String topicPrefixName,
      String schemaRegistryUrl,
      Map<String, Object> kafkaProducerSettings,
      MetricsConfig metricsConfig) {
    this.topicPrefixName = topicPrefixName;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.kafkaProducerSettings = ImmutableMap.copyOf(kafkaProducerSettings);
    this.metricsConfig = metricsConfig;
  }

  /**
   * @return The prefix that will be used for every kafka topic. For more information, see {@link
   *     DefaultMappingService}.
   */
  public String getTopicPrefixName() {
    return topicPrefixName;
  }

  /**
   * @return all kafka producer settings that are passed directly to the {@link
   *     CompletableKafkaProducer} when it is constructed. All settings that are passed to a kafka
   *     producer, should be prefixed with {@link ConfigLoader#CDC_KAFKA_PRODUCER_SETTING_PREFIX}.
   */
  public Map<String, Object> getKafkaProducerSettings() {
    return kafkaProducerSettings;
  }

  /**
   * @return the url that is used to connect to schema registry. The {@link SchemaRegistryProvider}
   *     uses it when constructing and retrieving schema. It will be passed to a kafka producer as
   *     {@link ConfigLoader#SCHEMA_REGISTRY_URL_SETTING_NAME}' setting. This settings should be
   *     prefixed with {@link ConfigLoader#CDC_KAFKA_PRODUCER_SETTING_PREFIX}
   */
  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  /**
   * @return MetricsConfiguration used to expose kafka producer settings to a {@link
   *     MetricRegistry}.
   */
  public MetricsConfig getMetricsConfig() {
    return metricsConfig;
  }
}
