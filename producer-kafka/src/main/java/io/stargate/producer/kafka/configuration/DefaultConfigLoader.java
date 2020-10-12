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

import edu.umd.cs.findbugs.annotations.NonNull;
import io.dropwizard.kafka.metrics.DropwizardMetricsReporter;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;

public class DefaultConfigLoader implements ConfigLoader {

  private static final Pattern CDC_KAFKA_PRODUCER_SETTING_PATTERN =
      Pattern.compile(String.format("^(%s)\\.(.*.)", CDC_KAFKA_PRODUCER_SETTING_PREFIX));

  @Override
  public CDCKafkaConfig loadConfig(Map<String, Object> options) {
    String topicPrefixName = getTopicPrefixName(options);
    Map<String, Object> kafkaProducerSettings = filterKafkaProducerSettings(options);
    MetricsConfig metricsConfig = loadMetricsConfig(options);
    registerMetricsIfEnabled(kafkaProducerSettings, metricsConfig);
    String schemaRegistryUrl = getSchemaRegistryUrl(kafkaProducerSettings);
    return new CDCKafkaConfig(
        topicPrefixName, schemaRegistryUrl, kafkaProducerSettings, metricsConfig);
  }

  private void registerMetricsIfEnabled(
      Map<String, Object> kafkaProducerSettings, MetricsConfig metricsConfig) {
    if (metricsConfig.isMetricsEnabled()) {
      kafkaProducerSettings.put(
          CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
          DropwizardMetricsReporter.class.getName());
      // map to  dropwizard specific settings
      kafkaProducerSettings.put(
          DropwizardMetricsReporter.SHOULD_INCLUDE_TAGS_CONFIG,
          Boolean.toString(metricsConfig.isIncludeTags()));
      kafkaProducerSettings.put(
          DropwizardMetricsReporter.METRICS_NAME_CONFIG, metricsConfig.getMetricsName());
    }
  }

  @NonNull
  MetricsConfig loadMetricsConfig(Map<String, Object> options) {
    boolean metricsEnabled = getBooleanSettingValue(options, METRICS_ENABLED_SETTING_NAME);
    // METRICS_INCLUDE_TAGS_SETTING_NAME and METRICS_NAME_SETTING_NAME are optional.
    // If not provided, the default value will be taken.
    Optional<Boolean> metricsIncludeTags =
        getOptionalBooleanSettingValue(options, METRICS_INCLUDE_TAGS_SETTING_NAME);
    Optional<String> metricsName = getOptionalStringValue(options, METRICS_NAME_SETTING_NAME);
    return MetricsConfig.create(metricsEnabled, metricsIncludeTags, metricsName);
  }

  Map<String, Object> filterKafkaProducerSettings(Map<String, Object> options) {
    return options.entrySet().stream()
        .filter(this::isKafkaProducerSetting)
        .map(this::toKafkaProducerSetting)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @NonNull
  Entry<String, Object> toKafkaProducerSetting(Entry<String, Object> entry) {
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

  String getTopicPrefixName(Map<String, Object> options) {
    return getStringSettingValue(options, CDC_TOPIC_PREFIX_NAME);
  }

  String getSchemaRegistryUrl(Map<String, Object> kafkaProducerSettings) {
    return getStringSettingValue(kafkaProducerSettings, SCHEMA_REGISTRY_URL_SETTING_NAME);
  }

  @NonNull
  String getStringSettingValue(Map<String, Object> options, String settingName) {
    return (String) getSettingValue(options, settingName, String.class);
  }

  @NonNull
  Optional<String> getOptionalStringValue(Map<String, Object> options, String settingName) {
    return getOptionalSettingValue(options, settingName, String.class).map(v -> (String) v);
  }

  @NonNull
  Boolean getBooleanSettingValue(Map<String, Object> options, String settingName) {
    return (Boolean) getSettingValue(options, settingName, Boolean.class);
  }

  @NonNull
  Optional<Boolean> getOptionalBooleanSettingValue(
      Map<String, Object> options, String settingName) {
    return getOptionalSettingValue(options, settingName, Boolean.class).map(v -> (Boolean) v);
  }

  @NonNull
  Object getSettingValue(Map<String, Object> options, String settingName, Class<?> expectedType) {
    Object configValue = options.get(settingName);
    if (configValue == null) {
      throw new IllegalArgumentException(
          String.format("The config value for %s is not present", settingName));
    }
    if (!(configValue.getClass().isAssignableFrom(expectedType))) {
      throw new IllegalArgumentException(
          String.format(
              "The config value for %s has wrong type: %s. It should be of a %s type",
              settingName, configValue.getClass().getName(), expectedType.getName()));
    }
    return configValue;
  }

  @NonNull
  Optional<Object> getOptionalSettingValue(
      Map<String, Object> options, String settingName, Class<?> expectedType) {
    Object configValue = options.get(settingName);
    if (configValue == null) {
      return Optional.empty();
    }
    if (!(configValue.getClass().isAssignableFrom(expectedType))) {
      throw new IllegalArgumentException(
          String.format(
              "The config value for %s has wrong type: %s. It should be of a %s type",
              settingName, configValue.getClass().getName(), expectedType.getName()));
    }
    return Optional.of(configValue);
  }
}
