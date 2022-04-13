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

package io.stargate.sgv2.docsapi.config.constants;

/** Static constants. */
public interface Constants {

  /** Name for the Open API default security scheme. */
  String OPEN_API_DEFAULT_SECURITY_SCHEME = "Token";

  /** Authentication token header name. */
  String AUTHENTICATION_TOKEN_HEADER_NAME = "X-Cassandra-Token";

  /** Tenant identifier header name. */
  String TENANT_ID_HEADER_NAME = "X-Tenant-Id";
}
