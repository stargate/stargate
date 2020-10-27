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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ConfigWithOverrides {
  private Map<String, Object> configMap;

  public ConfigWithOverrides(@Nonnull Map<String, Object> configMap) {
    this.configMap = configMap;
  }

  /**
   * It returns the underlying config-map without any override applied. If you want to get a
   * specific value with override, please use the {@link this#getWithOverrides(String)} method.
   */
  public Map<String, Object> getConfigMap() {
    return configMap;
  }

  /**
   * It returns a specific setting value for settingName.
   *
   * <p>It looks for settings in the following order:
   *
   * <p>1. Java System property ({@code System.getProperty()}
   *
   * <p>2. OS environment variable
   *
   * <p>3. Underlying config map
   *
   * @return the value with the highest priority or null if there is no value associated with the
   *     given settingName.
   */
  @Nullable
  public Object getWithOverrides(String settingName) {
    String systemProperty = System.getProperty(settingName);
    if (systemProperty != null) {
      return systemProperty;
    }

    String envVariable = System.getenv(settingName);
    if (envVariable != null) {
      return envVariable;
    }
    return configMap.get(settingName);
  }
}
