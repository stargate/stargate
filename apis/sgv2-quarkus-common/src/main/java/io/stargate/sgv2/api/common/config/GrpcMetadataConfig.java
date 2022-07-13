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
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import javax.validation.constraints.NotBlank;

/** Configuration for the gRPC metadata passed to the Bridge. */
@ConfigMapping(prefix = "stargate.grpc-metadata")
public interface GrpcMetadataConfig {

  /**
   * @return Metadata key for passing the tenant-id to the Bridge. Defaults to {@value
   *     HttpConstants#TENANT_ID_HEADER_NAME}
   */
  @NotBlank
  @WithDefault(HttpConstants.TENANT_ID_HEADER_NAME)
  String tenantIdKey();

  /**
   * @return Metadata key for passing the cassandra token to the Bridge. Defaults to {@value
   *     HttpConstants#AUTHENTICATION_TOKEN_HEADER_NAME}
   */
  @NotBlank
  @WithDefault(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME)
  String cassandraTokenKey();
}
