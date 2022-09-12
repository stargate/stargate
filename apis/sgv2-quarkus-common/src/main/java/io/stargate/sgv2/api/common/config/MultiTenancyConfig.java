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
import io.stargate.sgv2.api.common.tenant.TenantResolver;
import io.stargate.sgv2.api.common.tenant.impl.FixedTenantResolver;
import io.stargate.sgv2.api.common.tenant.impl.SubdomainTenantResolver;
import java.util.Optional;
import javax.validation.Valid;
import javax.validation.constraints.Pattern;

/** Configuration mapping for the tenant resolver. */
@ConfigMapping(prefix = "stargate.multi-tenancy")
public interface MultiTenancyConfig {

  /** @return If multi-tenancy is enabled. */
  @WithDefault("false")
  boolean enabled();

  /** @return Tenant resolver in case the multi-tenancy is active. */
  @Valid
  TenantResolverConfig tenantResolver();

  /** Configuration mapping for the tenant resolver. */
  interface TenantResolverConfig {

    /**
     * Tenant resolver type, possible options:
     *
     * <ol>
     *   <li><code>subdomain</code> - reads tenant id from request host domain (see {@link
     *       SubdomainTenantResolver}
     *   <li><code>fixed</code> - fixed tenant id supplied by the configuration (see {@link
     *       FixedTenantResolver}}
     *   <li><code>custom</code> - allows configuring custom tenant resolver
     * </ol>
     *
     * If unset, noop resolver will be used.
     *
     * @return The type of the {@link TenantResolver} used.
     */
    Optional<@Pattern(regexp = "subdomain|fixed|custom") String> type();

    /** @return Specific settings for the <code>fixed</code> tenant resolver type. */
    @Valid
    MultiTenancyConfig.TenantResolverConfig.FixedTenantResolverConfig fixed();

    interface FixedTenantResolverConfig {

      /** @return Tenant ID value. */
      Optional<String> tenantId();
    }
  }
}
