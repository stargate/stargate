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

package io.stargate.sgv2.docsapi.api.common.tenant.configuration;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.stargate.sgv2.docsapi.api.common.tenant.TenantResolver;
import io.stargate.sgv2.docsapi.api.common.tenant.impl.FixedTenantResolver;
import io.stargate.sgv2.docsapi.api.common.tenant.impl.SubdomainTenantResolver;
import io.stargate.sgv2.docsapi.config.TenantResolverConfig;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.Optional;

/**
 * Configuration for activating a correct {@link TenantResolver}.
 */
public class TenantConfiguration {

    @Produces
    @ApplicationScoped
    @LookupIfProperty(
            name = "stargate.tenant-resolver.type",
            stringValue = "subdomain"
    )
    TenantResolver subdomainTenantResolver() {
        return new SubdomainTenantResolver();
    }

    @Produces
    @ApplicationScoped
    @LookupIfProperty(
            name = "stargate.tenant-resolver.type",
            stringValue = "fixed"
    )
    TenantResolver fixedTenantResolver(TenantResolverConfig config) {
        return new FixedTenantResolver(config.fixed());
    }

    @Produces
    @ApplicationScoped
    @LookupIfProperty(
            name = "stargate.tenant-resolver.type",
            stringValue = "noop",
            lookupIfMissing = true
    )
    TenantResolver noopTenantResolver() {
        return (context, securityContext) -> Optional.empty();
    }

}
