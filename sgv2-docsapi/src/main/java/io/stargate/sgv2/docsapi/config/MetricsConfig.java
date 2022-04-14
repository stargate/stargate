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

package io.stargate.sgv2.docsapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.docsapi.config.constants.Constants;

import javax.validation.constraints.NotBlank;
import java.util.Map;

/** Extra, Stargate related configuration for the metrics. */
@ConfigMapping(prefix = "stargate.metrics")
public interface MetricsConfig {

  /**
   * @return Global tags attached to each metric being recorded.
   */
  Map<String, String> globalTags();

}
