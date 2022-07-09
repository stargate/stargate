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

package io.stargate.sgv2.docsapi.api.common.security.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.stargate.sgv2.docsapi.api.common.security.HeaderAuthenticationRequest;
import io.stargate.sgv2.docsapi.api.common.security.HeaderBasedAuthenticationMechanism;
import io.stargate.sgv2.docsapi.api.common.security.HeaderIdentityProvider;
import io.stargate.sgv2.docsapi.config.AuthConfig;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

/** The configuration for the header based security. */
public class HeaderBasedSecurityConfiguration {

  @Produces
  @ApplicationScoped
  @LookupIfProperty(name = "stargate.auth.header-based.enabled", stringValue = "true")
  HttpAuthenticationMechanism httpAuthenticationMechanism(
      AuthConfig config, ObjectMapper objectMapper) {
    String headerName = config.headerBased().headerName();
    return new HeaderBasedAuthenticationMechanism(headerName, objectMapper);
  }

  @Produces
  @ApplicationScoped
  @LookupIfProperty(name = "stargate.auth.header-based.enabled", stringValue = "true")
  IdentityProvider<HeaderAuthenticationRequest> identityProvider() {
    return new HeaderIdentityProvider();
  }
}
