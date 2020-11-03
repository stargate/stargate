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

public interface ConfigStore {

  /**
   * It retrieves the generic {@code Map<String, Object>} for a given module name. The map is
   * wrapped in the {@link ConfigWithOverrides} to allow the caller to get the setting value
   * overridable by system property or environment variable.
   *
   * <p>If the caller will try to look up the config for a module that does not have any setting, it
   * will throw the {@link MissingModuleSettingsException}. This is a {@link RuntimeException} and
   * it's the caller's responsibility to catch it and handle gracefully or propagate upper in the
   * call stack.
   */
  ConfigWithOverrides getConfigForModule(String moduleName) throws MissingModuleSettingsException;

  /**
   * This is a convenience method that calls the {@link this#getConfigForModule(String)} with a
   * stargate module name.
   */
  default ConfigWithOverrides getGlobalConfig() {
    return getConfigForModule("stargate");
  }
}
