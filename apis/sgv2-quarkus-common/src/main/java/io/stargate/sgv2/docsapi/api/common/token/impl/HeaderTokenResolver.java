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

package io.stargate.sgv2.docsapi.api.common.token.impl;

import io.stargate.sgv2.docsapi.api.common.token.CassandraTokenResolver;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import javax.ws.rs.core.SecurityContext;

/** The {@link CassandraTokenResolver} that resolves a token from the HTTP header. */
public class HeaderTokenResolver implements CassandraTokenResolver {

  /** The name of the header to extract the token from. */
  private final String headerName;

  public HeaderTokenResolver(String headerName) {
    this.headerName = headerName;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<String> resolve(RoutingContext context, SecurityContext securityContext) {
    HttpServerRequest request = context.request();
    String headerValue = request.getHeader(headerName);
    return Optional.ofNullable(headerValue);
  }
}
