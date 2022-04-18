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
import io.stargate.sgv2.docsapi.api.common.properties.model.DataStoreProperties;

/**
 * Configuration for the data store.
 *
 * <p><b>IMPORTANT:</b> Do not inject this class, but rather {@link
 * io.stargate.sgv2.docsapi.api.common.properties.model.CombinedProperties}.
 */
@ConfigMapping(prefix = "stargate.data-store")
public interface DataStoreConfig extends DataStoreProperties {

  /**
   * @return If the data store config should be read by the bridge, defaults to <code>false</code>.
   */
  @WithDefault("false")
  boolean readFromBridge();

  /** @return If the secondary indexes are enabled, defaults to <code>true</code>. */
  @WithDefault("true")
  boolean secondaryIndexesEnabled();

  /** @return If storage attached indexes are enabled, defaults to <code>false</code>. */
  @WithDefault("false")
  boolean saiEnabled();

  /** @return If logged batches are enabled, defaults to <code>true</code>. */
  @WithDefault("true")
  boolean loggedBatchesEnabled();
}
