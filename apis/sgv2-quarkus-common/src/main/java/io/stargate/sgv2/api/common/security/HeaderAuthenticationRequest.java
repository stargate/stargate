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

package io.stargate.sgv2.api.common.security;

import io.quarkus.security.identity.request.BaseAuthenticationRequest;

/** {@link io.quarkus.security.identity.request.AuthenticationRequest} based on the HTTP header. */
public class HeaderAuthenticationRequest extends BaseAuthenticationRequest {

  /** The name of the header that was used for the authentication request. */
  private final String headerName;

  /** The value of the header that was used for the authentication request. */
  private final String headerValue;

  public HeaderAuthenticationRequest(String headerName, String headerValue) {
    this.headerName = headerName;
    this.headerValue = headerValue;
  }

  public String getHeaderName() {
    return headerName;
  }

  public String getHeaderValue() {
    return headerValue;
  }
}
