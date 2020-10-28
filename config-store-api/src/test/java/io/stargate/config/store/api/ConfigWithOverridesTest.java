/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.config.store.api;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConfigWithOverridesTest {

  private static final String SETTING_NAME = "key_" + UUID.randomUUID().toString();

  @ParameterizedTest
  @MethodSource("getSettingsProvider")
  public void shouldReturnSettingWithTheHighestPriority(
      Map<String, Object> configMap, String envVariable, String systemProperty, Object expected) {
    try {
      // given
      setEnv(SETTING_NAME, envVariable);
      if (systemProperty != null) {
        System.setProperty(SETTING_NAME, systemProperty);
      }

      ConfigWithOverrides configWithOverrides = new ConfigWithOverrides(configMap);

      // when
      Object result = configWithOverrides.getWithOverrides(SETTING_NAME);

      // then
      assertThat(result).isEqualTo(expected);

      // and then
      assertThat(configWithOverrides.getConfigMap()).isEqualTo(configMap);
    } finally {
      System.clearProperty(SETTING_NAME);
      clearEnv(SETTING_NAME);
    }
  }

  public static Stream<Arguments> getSettingsProvider() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(SETTING_NAME, "config_value");

    return Stream.of(
        Arguments.of(configMap, null, null, "config_value"),
        Arguments.of(configMap, "env_value", null, "env_value"),
        Arguments.of(configMap, null, "system_property", "system_property"),
        Arguments.of(configMap, "env_value", "system_property", "system_property"),
        Arguments.of(Collections.emptyMap(), null, null, null));
  }

  private static void setEnv(String key, String value) {
    // do not set if env variable is null
    if (value == null) {
      return;
    }
    try {
      Map<String, String> writableEnv = getEnvMap();
      writableEnv.put(key, value);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to set environment variable", e);
    }
  }

  private static void clearEnv(String key) {
    try {
      Map<String, String> writableEnv = getEnvMap();
      writableEnv.remove(key);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to set environment variable", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> getEnvMap()
      throws NoSuchFieldException, IllegalAccessException {
    Map<String, String> env = System.getenv();
    Class<?> cl = env.getClass();
    Field field = cl.getDeclaredField("m");
    field.setAccessible(true);
    return (Map<String, String>) field.get(env);
  }
}
