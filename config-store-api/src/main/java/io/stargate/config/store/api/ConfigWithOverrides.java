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

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ConfigWithOverrides {
  private final Map<String, Object> configMap;
  private final String moduleName;

  public ConfigWithOverrides(@Nonnull Map<String, Object> configMap, String moduleName) {
    this.configMap = configMap;
    this.moduleName = moduleName;
  }

  /**
   * It returns the underlying config-map without any override applied. If you want to get a
   * specific value with override, please use the {@link this#getWithOverrides(String)} method.
   */
  public Map<String, Object> getConfigMap() {
    return configMap;
  }

  @Nonnull
  String getStringSettingValue(String settingName) {
    return (String) getSettingValue(settingName, String.class);
  }

  @Nonnull
  Optional<String> getOptionalStringValue(String settingName) {
    return getOptionalSettingValue(settingName, String.class).map(v -> (String) v);
  }

  @Nonnull
  Boolean getBooleanSettingValue(String settingName) {
    return (Boolean) getSettingValue(settingName, Boolean.class);
  }

  @Nonnull
  Optional<Boolean> getOptionalBooleanSettingValue(String settingName) {
    return getOptionalSettingValue(settingName, Boolean.class).map(v -> (Boolean) v);
  }

  @Nonnull
  Object getSettingValue(String settingName, Class<?> expectedType) {
    Object configValue = configMap.get(settingName);
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

  @Nonnull
  Optional<Object> getOptionalSettingValue(String settingName, Class<?> expectedType) {
    Object configValue = configMap.get(settingName);
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

  /**
   * It returns a specific setting value for settingName.
   *
   * <p>It looks for a setting value in the following order:
   *
   * <p>1. Java System property ({@code System.getProperty()}
   *
   * <p>2. OS environment variable
   *
   * <p>3. Underlying config map
   *
   * <p>When trying to retrieve the system property and environment variable, it will use the full
   * setting name. It will add the module name for which this `ConfigWithOverrides` is created.
   *
   * <p>For example, if the {@code String moduleName = "m_1"} and you are calling this
   * getWithOverrides() method for settingName = "s" it will firstly try to {@code
   * System.getProperty("m_1.s")}, then {@code System.getenv("m_1.s")} and finally get the config
   * from the underlying map using {@code Map.get("s")}.
   *
   * <p>Prefixing with module name is done to avoid conflicts of overrides between modules.
   *
   * <p>Please keep in mind that if you are overriding settings via a System property or OS
   * environment variable, it will always return the String value. If the underlying config map does
   * not contain a String for the specific setting name, and the override is provided, you may get
   * class cast problems. To alleviate this problem, you should assert that the underlying config
   * map value for the setting that you plan to override is of a String type. You can also add a
   * custom parsing logic with instanceof checks but it may be error-prone.
   *
   * @return the value with the highest priority or null if there is no value associated with the
   *     given settingName.
   */
  @Nullable
  public Object getWithOverrides(String settingName) {
    String settingNameWithModulePrefix = withModulePrefix(settingName);
    String systemProperty = System.getProperty(settingNameWithModulePrefix);
    if (systemProperty != null) {
      return systemProperty;
    }

    String envVariable = System.getenv(settingNameWithModulePrefix);
    if (envVariable != null) {
      return envVariable;
    }
    return configMap.get(settingName);
  }

  private String withModulePrefix(String settingName) {
    return String.format("%s.%s", moduleName, settingName);
  }
}
