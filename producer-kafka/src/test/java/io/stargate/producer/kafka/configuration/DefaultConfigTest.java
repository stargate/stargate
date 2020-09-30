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

import static io.stargate.producer.kafka.configuration.ConfigLoader.*;
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
    CDCKafkaConfig cdcKafkaConfig = new DefaultConfigLoader().loadConfig(options);

    // then
    assertThat(cdcKafkaConfig.getTopicPrefixName()).isEqualTo("some-prefix");
  }

  @Test
  public void shouldThrowIfPrefixIsNull() {
    // given
    Map<String, Object> options = new HashMap<>();
    options.put(CDC_TOPIC_PREFIX_NAME, null);

    // when, then
    assertThatThrownBy(() -> new DefaultConfigLoader().loadConfig(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format("The config value for %s is not present", CDC_TOPIC_PREFIX_NAME));
  }

  @Test
  public void shouldThrowIfPrefixHasWrongType() {
    // given
    Map<String, Object> options = new HashMap<>();
    options.put(CDC_TOPIC_PREFIX_NAME, 1234);

    // when, then
    assertThatThrownBy(() -> new DefaultConfigLoader().loadConfig(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The config value for %s has wrong type: %s. It should be of a String type",
                CDC_TOPIC_PREFIX_NAME, Integer.class.getName()));
  }

  @Test
  public void shouldExtractKafkaProducerSettings() {
    // given
    Map<String, Object> options = new HashMap<>();
    options.put(CDC_TOPIC_PREFIX_NAME, "not-relevant");
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
        .containsExactly(new SimpleEntry<>("setting-a", 1), new SimpleEntry<>("setting-b", "a"));
  }
}
