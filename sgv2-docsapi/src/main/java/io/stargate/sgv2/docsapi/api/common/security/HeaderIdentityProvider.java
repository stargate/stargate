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

package io.stargate.sgv2.docsapi.api.common.security;

import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;

/**
 * Identity provider that works with the {@link HeaderAuthenticationRequest}.
 * <p>
 * Note that this provider creates an identity with a {@link java.security.Principal} containing the value of the authentication header.
 */
public class HeaderIdentityProvider implements IdentityProvider<HeaderAuthenticationRequest> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<HeaderAuthenticationRequest> getRequestType() {
        return HeaderAuthenticationRequest.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Uni<SecurityIdentity> authenticate(HeaderAuthenticationRequest request, AuthenticationRequestContext context) {
        // no explicit authentication, the existence of the HeaderAuthenticationRequest is enough
        QuarkusPrincipal headerValuePrincipal = new QuarkusPrincipal(request.getHeaderValue());
        QuarkusSecurityIdentity identity = QuarkusSecurityIdentity.builder().setPrincipal(headerValuePrincipal).build();
        return Uni.createFrom().item(identity);
    }
}
