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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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

  /**
   * It retrieves the list of values from underlying configMap checking for its presence and the
   * type. If the value for a given settingName does not have a value, it throws an {@code
   * IllegalArgumentException}. If the type of the setting value is not a {@link List} it throws
   * {@code IllegalArgumentException}. If the underlying type of any element in this list does not
   * match the expectedType, it throws {@code IllegalArgumentException}.
   *
   * @param settingName - it will be used as a key of underlying configMap.
   * @param expectedType - it will be used to check if the type of an actual list element value
   *     matches.
   * @return the list of values associated with settingName. Each value matches expectedType.
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  public <T> List<T> getSettingValueList(String settingName, Class<T> expectedType) {
    List<?> settingValue = getSettingValue(settingName, List.class);
    return settingValue.stream()
        .map(
            v -> {
              validateType(String.format("%s.list-value", settingName), v, expectedType);
              return (T) v;
            })
        .collect(Collectors.toList());
  }

  /**
   * It retrieves the value from underlying configMap checking for its presence and type. If the
   * value for a given settingName does not have a value, it throws an {@code
   * IllegalArgumentException}. If the type of the setting value does not match the expectedType, it
   * throws {@code IllegalArgumentException}.
   *
   * @param settingName - it will be used as a key of underlying configMap.
   * @param expectedType - it will be used to check if the type of an actual setting value matches.
   * @return the value associated with settingName, matching expectedType.
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  public <T> T getSettingValue(String settingName, Class<T> expectedType) {
    Object configValue = configMap.get(settingName);
    if (configValue == null) {
      throw new IllegalArgumentException(
          String.format("The config value for %s is not present", settingName));
    }
    validateType(settingName, configValue, expectedType);
    return (T) configValue;
  }

  /**
   * It retrieves the value wrapped in the {@code Optional} from underlying configMap checking for
   * its type. If the value for a given settingName does not have a value, it returns {@code
   * Optional.empty()} If the type of the setting value does not match the expectedType, it throws
   * {@code IllegalArgumentException}.
   *
   * @param settingName - it will be used as a key of underlying configMap.
   * @param expectedType - it will be used to check if the type of an actual setting value matches.
   * @return the value wrapped in the {@code Optional} associated with settingName, matching
   *     expectedType.
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  public <T> Optional<T> getOptionalSettingValue(String settingName, Class<T> expectedType) {
    Object configValue = configMap.get(settingName);
    if (configValue == null) {
      return Optional.empty();
    }
    validateType(settingName, configValue, expectedType);
    return Optional.of(configValue).map(v -> (T) v);
  }

  private <T> void validateType(String settingName, Object configValue, Class<T> expectedType) {
    if (!(expectedType.isAssignableFrom(configValue.getClass()))) {
      throw new IllegalArgumentException(
          String.format(
              "The config value for %s has wrong type: %s. It should be of a %s type",
              settingName, configValue.getClass().getName(), expectedType.getName()));
    }
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
