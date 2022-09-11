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
import io.stargate.bridge.proto.QueryOuterClass;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/** Queries configuration. */
@ConfigMapping(prefix = "stargate.queries")
public interface QueriesConfig {

  /** @return Settings for the consistency level. */
  @Valid
  QueriesConfig.ConsistencyConfig consistency();

  interface ConsistencyConfig {

    /** @return Consistency for queries making schema changes. */
    @WithDefault("LOCAL_QUORUM")
    @NotNull
    QueryOuterClass.Consistency schemaChanges();

    /** @return Consistency for queries writing the data. */
    @WithDefault("LOCAL_QUORUM")
    @NotNull
    QueryOuterClass.Consistency writes();

    /** @return Consistency for queries reading the data. */
    @WithDefault("LOCAL_QUORUM")
    @NotNull
    QueryOuterClass.Consistency reads();
  }
}
