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

package io.stargate.sgv2.api.common.tenant.impl;

import io.stargate.sgv2.api.common.config.MultiTenancyConfig;
import io.stargate.sgv2.api.common.tenant.TenantResolver;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import javax.ws.rs.core.SecurityContext;

/** The {@link TenantResolver} that uses a fixed tenant ID supplied by the configuration. */
public class FixedTenantResolver implements TenantResolver {

  private final MultiTenancyConfig.TenantResolverConfig.FixedTenantResolverConfig config;

  public FixedTenantResolver(
      MultiTenancyConfig.TenantResolverConfig.FixedTenantResolverConfig config) {
    this.config = config;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<String> resolve(RoutingContext context, SecurityContext securityContext) {
    return config.tenantId();
  }
}
