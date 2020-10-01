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
import static io.stargate.producer.kafka.configuration.ConfigLoader.SCHEMA_REGISTRY_URL_SETTING_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
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
                "The config value for %s has wrong type: %s. It should be of a String type",
                settingName, Integer.class.getName()));
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

    // when
    CDCKafkaConfig config = new DefaultConfigLoader().loadConfig(options);

    // then
    assertThat(config.getKafkaProducerSettings())
        .containsExactly(
            new SimpleEntry<>("setting-a", 1),
            new SimpleEntry<>("setting-b", "a"),
            new SimpleEntry<>(SCHEMA_REGISTRY_URL_SETTING_NAME, "schema-url"));
    assertThat(config.getSchemaRegistryUrl()).isEqualTo("schema-url");
    assertThat(config.getTopicPrefixName()).isEqualTo("prefix");
  }
}
