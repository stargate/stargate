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

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import java.util.Optional;

/**
 * Configuration mapping for the tenant resolver.
 */
@ConfigMapping(prefix = "stargate.tenant-resolver")
public interface TenantResolverConfig {

    /**
     * Tenant resolver type, possible options:
     * <ol>
     *     <li><code>subdomain</code> - reads tenant id from request host domain (see {@link io.stargate.sgv2.docsapi.api.common.tenant.impl.SubdomainTenantResolver}</li>
     *     <li><code>fixed</code> - fixed tenant id supplied by the configuration (see {@link io.stargate.sgv2.docsapi.api.common.tenant.impl.FixedTenantResolver}}</li>
     *     <li><code>custom</code> - allows configuring custom tenant resolver</li>
     * </ol>
     * If unset, noop resolver will be used.
     *
     * @return The type of the {@link io.stargate.sgv2.docsapi.api.common.tenant.TenantResolver} used.
     */
    Optional<@Pattern(regexp = "subdomain|fixed|custom") String> type();

    /**
     * @return Specific settings for the <code>fixed</code> tenant resolver type.
     */
    @Valid
    TenantResolverConfig.FixedTenantResolverConfig fixed();

    interface FixedTenantResolverConfig {

        /**
         * @return Tenant ID value.
         */
        Optional<@NotBlank String> tenantId();

    }
}
