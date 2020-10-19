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

import static io.stargate.producer.kafka.configuration.ConfigLoader.CDC_KAFKA_PRODUCER_SETTING_PREFIX;
import static io.stargate.producer.kafka.configuration.ConfigLoader.CDC_TOPIC_PREFIX_NAME;
import static io.stargate.producer.kafka.configuration.ConfigLoader.METRICS_ENABLED_SETTING_NAME;
import static io.stargate.producer.kafka.configuration.ConfigLoader.METRICS_INCLUDE_TAGS_SETTING_NAME;
import static io.stargate.producer.kafka.configuration.ConfigLoader.METRICS_NAME_SETTING_NAME;
import static io.stargate.producer.kafka.configuration.ConfigLoader.SCHEMA_REGISTRY_URL_SETTING_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.dropwizard.kafka.metrics.DropwizardMetricsReporter;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

class DefaultConfigTest {

  @Test
  public void shouldExtractPrefixName() {
    // given
    Map<String, Object> options = new HashMap<>();
    options.put(CDC_TOPIC_PREFIX_NAME, "some-prefix");

    // when
    String topicPrefixName = new DefaultConfigLoader().getTopicPrefixName(options);

    // then
    assertThat(topicPrefixName).isEqualTo("some-prefix");
  }

  @Test
  public void shouldExtractKafkaProducerSettings() {
    // given
    Map<String, Object> options = new HashMap<>();
    options.put(String.format("%s.%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, "setting-a"), 1);
    options.put(String.format("%s.%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, "setting-b"), "a");
    options.put(
        String.format("%s-wrong.%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, "setting"), "ignored");
    options.put(String.format(".%s.%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, "setting"), "ignored");
    options.put(String.format(".%s%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, "setting"), "ignored");
    options.put(String.format("%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX), "ignored");

    // when
    Map<String, Object> config = new DefaultConfigLoader().filterKafkaProducerSettings(options);

    // then
    assertThat(config)
        .containsExactly(new SimpleEntry<>("setting-a", 1), new SimpleEntry<>("setting-b", "a"));
  }

  @Test
  public void shouldExtractSchemaRegistrySetting() {
    // given
    Map<String, Object> options = new HashMap<>();
    options.put(SCHEMA_REGISTRY_URL_SETTING_NAME, "url");

    // when
    String schemaRegistryUrl = new DefaultConfigLoader().getSchemaRegistryUrl(options);

    // then
    assertThat(schemaRegistryUrl).isEqualTo("url");
  }

  @Test
  public void shouldThrowIfValueIsNull() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, null);

    // when, then
    assertThatThrownBy(() -> new DefaultConfigLoader().getStringSettingValue(options, settingName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(String.format("The config value for %s is not present", settingName));
  }

  @Test
  public void shouldThrowIfValueHasWrongType() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, 1234);

    // when, then
    assertThatThrownBy(() -> new DefaultConfigLoader().getStringSettingValue(options, settingName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The config value for %s has wrong type: %s. It should be of a %s type",
                settingName, Integer.class.getName(), String.class.getName()));
  }

  @Test
  public void shouldConstructMetricsWithAllSettings() {
    // given
    Map<String, Object> options = new HashMap<>();
    options.put(METRICS_NAME_SETTING_NAME, "producer-prefix");
    options.put(METRICS_ENABLED_SETTING_NAME, true);
    options.put(METRICS_INCLUDE_TAGS_SETTING_NAME, true);

    // when
    MetricsConfig metricsConfig = new DefaultConfigLoader().loadMetricsConfig(options);

    // then
    assertThat(metricsConfig.isMetricsEnabled()).isEqualTo(true);
    assertThat(metricsConfig.getMetricsName()).isEqualTo("producer-prefix");
    assertThat(metricsConfig.isIncludeTags()).isEqualTo(true);
  }

  @Test
  public void shouldConstructMetricsWithDefaultsIfNotProvided() {
    // given
    Map<String, Object> options = new HashMap<>();
    options.put(METRICS_ENABLED_SETTING_NAME, true);

    // when
    MetricsConfig metricsConfig = new DefaultConfigLoader().loadMetricsConfig(options);

    // then
    assertThat(metricsConfig.isMetricsEnabled()).isEqualTo(true);
    assertThat(metricsConfig.getMetricsName()).isEqualTo(MetricsConfig.METRICS_NAME_DEFAULT);
    assertThat(metricsConfig.isIncludeTags()).isEqualTo(MetricsConfig.INCLUDE_TAGS_DEFAULT);
  }

  @Test
  public void shouldConstructCDCKafkaConfig() {
    // given
    Map<String, Object> options = new HashMap<>();
    options.put(CDC_TOPIC_PREFIX_NAME, "prefix");
    options.put(
        String.format("%s.%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, SCHEMA_REGISTRY_URL_SETTING_NAME),
        "schema-url");
    options.put(String.format("%s.%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, "setting-a"), 1);
    options.put(String.format("%s.%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, "setting-b"), "a");
    options.put(
        String.format("%s-wrong.%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, "setting"), "ignored");
    options.put(String.format(".%s.%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, "setting"), "ignored");
    options.put(String.format(".%s%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX, "setting"), "ignored");
    options.put(String.format("%s", CDC_KAFKA_PRODUCER_SETTING_PREFIX), "ignored");

    // metrics config
    options.put(METRICS_NAME_SETTING_NAME, "producer-prefix");
    options.put(METRICS_ENABLED_SETTING_NAME, true);
    options.put(METRICS_INCLUDE_TAGS_SETTING_NAME, true);
    // when
    CDCKafkaConfig config = new DefaultConfigLoader().loadConfig(options);

    // then
    assertThat(config.getKafkaProducerSettings())
        .containsOnly(
            new SimpleEntry<>("setting-a", 1),
            new SimpleEntry<>("setting-b", "a"),
            new SimpleEntry<>(SCHEMA_REGISTRY_URL_SETTING_NAME, "schema-url"),
            // metric specific settings
            new SimpleEntry<>(
                CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                DropwizardMetricsReporter.class.getName()),
            new SimpleEntry<>(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "producer-prefix"),
            new SimpleEntry<>(DropwizardMetricsReporter.SHOULD_INCLUDE_TAGS_CONFIG, "true"),
            new SimpleEntry<>(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class),
            new SimpleEntry<>(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class));
    assertThat(config.getSchemaRegistryUrl()).isEqualTo("schema-url");
    assertThat(config.getTopicPrefixName()).isEqualTo("prefix");
    assertThat(config.getMetricsConfig())
        .isEqualTo(new MetricsConfig(true, true, "producer-prefix"));
  }
}
