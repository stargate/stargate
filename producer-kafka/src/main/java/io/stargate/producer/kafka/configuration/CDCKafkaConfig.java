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

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class CDCKafkaConfig {
  private final String topicPrefixName;
  private final String schemaRegistryUrl;
  private final Map<String, Object> kafkaProducerSettings;

  public CDCKafkaConfig(
      String topicPrefixName, String schemaRegistryUrl, Map<String, Object> kafkaProducerSettings) {
    this.topicPrefixName = topicPrefixName;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.kafkaProducerSettings = ImmutableMap.copyOf(kafkaProducerSettings);
  }

  public String getTopicPrefixName() {
    return topicPrefixName;
  }

  public Map<String, Object> getKafkaProducerSettings() {
    return kafkaProducerSettings;
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }
}
