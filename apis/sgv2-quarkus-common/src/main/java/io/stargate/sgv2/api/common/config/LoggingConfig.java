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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.api.common.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Optional;
import java.util.Set;

import static io.stargate.sgv2.api.common.config.constants.LoggingConstants.*;

/** Extra, Stargate related configuration for the logging. */
@ConfigMapping(prefix = "stargate.api.logging")
public interface LoggingConfig {

  /** @return If request info logging is enabled. */
  @WithDefault(REQUEST_INFO_LOGGING_ENABLED)
  boolean enabled();

  /** @return Set of tenants for which the request info should be logged. */
  @WithDefault(ALL_TENANTS)
  Optional<Set<String>> enabledTenants();

  /** @return Set of paths for which the request info should be logged. */
  @WithDefault(ALL_PATHS)
  Optional<Set<String>> enabledPaths();

  /** @return Set of path prefixes for which the request info should be logged. */
  @WithDefault(ALL_PATH_PREFIXES)
  Optional<Set<String>> enabledPathPrefixes();

  /** @return Set of error codes for which the request info should be logged. */
  @WithDefault(ALL_ERROR_CODES)
  Optional<Set<String>> enabledErrorCodes();

  /** @return Set of methods for which the request info should be logged. */
  @WithDefault(ALL_METHODS)
  Optional<Set<String>> enabledMethods();

  /** @return If request body logging is enabled. */
  @WithDefault(REQUEST_BODY_LOGGING_ENABLED)
  boolean requestBodyLoggingEnabled();
}
