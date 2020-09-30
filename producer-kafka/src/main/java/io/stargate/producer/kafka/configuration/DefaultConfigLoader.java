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

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

public class DefaultConfigLoader implements ConfigLoader {

  private static final Pattern CDC_KAFKA_PRODUCER_SETTING_PATTERN =
      Pattern.compile(String.format("^(%s)\\.(.*.)", CDC_KAFKA_PRODUCER_SETTING_PREFIX));

  @Override
  public CDCKafkaConfig loadConfig(Map<String, Object> options) {
    String topicPrefixName = getTopicPrefixName(options);
    Map<String, Object> kafkaProducerSettings = filterKafkaProducerSettings(options);
    return new CDCKafkaConfig(topicPrefixName, kafkaProducerSettings);
  }

  private Map<String, Object> filterKafkaProducerSettings(Map<String, Object> options) {
    return options.entrySet().stream()
        .filter(this::isKafkaProducerSetting)
        .map(this::toKafkaProducerSetting)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @NotNull
  private Entry<String, Object> toKafkaProducerSetting(Entry<String, Object> entry) {
    Matcher matcher = CDC_KAFKA_PRODUCER_SETTING_PATTERN.matcher(entry.getKey());
    if (matcher.matches()) {
      return new SimpleEntry<>(matcher.group(2), entry.getValue());
    } else {
      throw new IllegalStateException(
          String.format(
              "Matcher for pattern: %s and entry.key: %s should match",
              CDC_KAFKA_PRODUCER_SETTING_PATTERN, entry.getKey()));
    }
  }

  private boolean isKafkaProducerSetting(Entry<String, Object> entry) {
    return CDC_KAFKA_PRODUCER_SETTING_PATTERN.matcher(entry.getKey()).matches();
  }

  private String getTopicPrefixName(Map<String, Object> options) {
    Object prefixName = options.get(CDC_TOPIC_PREFIX_NAME);
    if (prefixName == null) {
      throw new IllegalArgumentException(
          String.format("The config value for %s is not present", CDC_TOPIC_PREFIX_NAME));
    }
    if (!(prefixName instanceof String)) {
      throw new IllegalArgumentException(
          String.format(
              "The config value for %s has wrong type: %s. It should be of a String type",
              CDC_TOPIC_PREFIX_NAME, prefixName.getClass().getName()));
    }
    return (String) prefixName;
  }
}
