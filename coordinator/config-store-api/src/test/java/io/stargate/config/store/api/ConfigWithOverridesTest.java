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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConfigWithOverridesTest {

  private static final String SETTING_NAME = "key_" + UUID.randomUUID();

  private static final String MODULE_NAME = "module_1";

  private static final String FULL_SETTING_NAME = String.format("%s.%s", MODULE_NAME, SETTING_NAME);

  @ParameterizedTest
  @MethodSource("getSettingsProvider")
  public void shouldReturnSettingWithTheHighestPriority(
      Map<String, Object> configMap, String envVariable, String systemProperty, Object expected) {
    try {
      // given
      setEnv(FULL_SETTING_NAME, envVariable);
      if (systemProperty != null) {
        System.setProperty(FULL_SETTING_NAME, systemProperty);
      }

      ConfigWithOverrides configWithOverrides = new ConfigWithOverrides(configMap, MODULE_NAME);

      // when
      String result = configWithOverrides.getWithOverrides(SETTING_NAME);

      // then
      assertThat(result).isEqualTo(expected);

      // and then
      assertThat(configWithOverrides.getConfigMap()).isEqualTo(configMap);
    } finally {
      System.clearProperty(FULL_SETTING_NAME);
      clearEnv(FULL_SETTING_NAME);
    }
  }

  @Test
  public void shouldMapFromBooleanToStringUsingMapperFunctionPassedToGetWithOverrides() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, true);

    // when
    String value =
        new ConfigWithOverrides(options, "ignored").getWithOverrides(settingName, String::valueOf);

    // then
    assertThat(value).isEqualTo("true");
  }

  @Test
  public void shouldSuccessfullyGetTheRequiredStringValue() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, "v");

    // when
    String value =
        new ConfigWithOverrides(options, "ignored").getSettingValue(settingName, String.class);

    // then
    assertThat(value).isEqualTo("v");
  }

  @Test
  public void shouldSuccessfullyGetRequiredBooleanValue() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, true);

    // when
    boolean value =
        new ConfigWithOverrides(options, "ignored").getSettingValue(settingName, Boolean.class);

    // then
    assertThat(value).isEqualTo(true);
  }

  @Test
  public void shouldSuccessfullyGetRequiredListValue() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    List<Integer> expected = Arrays.asList(1, 2, 3);
    options.put(settingName, expected);

    // when
    List<Integer> value =
        new ConfigWithOverrides(options, "ignored").getSettingValueList(settingName, Integer.class);

    // then
    assertThat(value).isEqualTo(expected);
  }

  @Test
  public void shouldSuccessfullyGetRequiredEmptyListValue() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    List<Integer> expected = Collections.emptyList();
    options.put(settingName, expected);

    // when
    List<Integer> value =
        new ConfigWithOverrides(options, "ignored").getSettingValueList(settingName, Integer.class);

    // then
    assertThat(value).isEqualTo(expected);
  }

  @Test
  public void shouldSuccessfullyGetTheOptionalStringValue() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, "v");

    // when
    Optional<String> value =
        new ConfigWithOverrides(options, "ignored")
            .getOptionalSettingValue(settingName, String.class);

    // then
    assertThat(value.get()).isEqualTo("v");
  }

  @Test
  public void shouldSuccessfullyGetRequiredOptionalBooleanValue() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, true);

    // when
    Optional<Boolean> value =
        new ConfigWithOverrides(options, "ignored")
            .getOptionalSettingValue(settingName, Boolean.class);

    // then
    assertThat(value.get()).isEqualTo(true);
  }

  @Test
  public void shouldThrowIfValueIsNullForRequiredGetter() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, null);

    // when, then
    assertThatThrownBy(
            () ->
                new ConfigWithOverrides(options, "ignored")
                    .getSettingValue(settingName, String.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(String.format("The config value for %s is not present", settingName));
  }

  @Test
  public void shouldThrowIfValueIsNullForRequiredListGetter() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, null);

    // when, then
    assertThatThrownBy(
            () ->
                new ConfigWithOverrides(options, "ignored")
                    .getSettingValueList(settingName, String.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(String.format("The config value for %s is not present", settingName));
  }

  @Test
  public void shouldNotThrowIfValueIsNullForOptionalGetter() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, null);

    // when, then
    assertThat(
            new ConfigWithOverrides(options, "ignored")
                .getOptionalSettingValue(settingName, String.class)
                .isPresent())
        .isFalse();
  }

  @Test
  public void shouldThrowIfValueHasWrongType() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, 1234);

    // when, then
    assertThatThrownBy(
            () ->
                new ConfigWithOverrides(options, "ignored")
                    .getSettingValue(settingName, String.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The config value for %s has wrong type: %s. It should be of a %s type",
                settingName, Integer.class.getName(), String.class.getName()));
  }

  @Test
  public void shouldThrowIfListHasWrongType() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, 1234);

    // when, then
    assertThatThrownBy(
            () ->
                new ConfigWithOverrides(options, "ignored")
                    .getSettingValueList(settingName, String.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The config value for %s has wrong type: %s. It should be of a %s type",
                settingName, Integer.class.getName(), List.class.getName()));
  }

  @Test
  public void shouldThrowIfListValueHasWrongType() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, Collections.singletonList(1));

    // when, then
    assertThatThrownBy(
            () ->
                new ConfigWithOverrides(options, "ignored")
                    .getSettingValueList(settingName, String.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The config value for %s.list-value has wrong type: %s. It should be of a %s type",
                settingName, Integer.class.getName(), String.class.getName()));
  }

  @Test
  public void shouldThrowIfOptionalValueHasWrongType() {
    // given
    Map<String, Object> options = new HashMap<>();
    String settingName = "setting-a";
    options.put(settingName, 1234);

    // when, then
    assertThatThrownBy(
            () ->
                new ConfigWithOverrides(options, "ignored")
                    .getOptionalSettingValue(settingName, String.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The config value for %s has wrong type: %s. It should be of a %s type",
                settingName, Integer.class.getName(), String.class.getName()));
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
