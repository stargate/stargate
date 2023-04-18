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
import java.util.regex.Pattern;
import jakarta.ws.rs.core.SecurityContext;

/**
 * {@link TenantResolver} that finds the tenant ID in the left most domain part of the host name.
 *
 * <p>For example, having <code>tenant-id.domain.com</code> will resolve tenant identifier to the
 * <code>tenant-id</code>. In case of top-level domain, <code>domain.com</code> will resolve tenant
 * identifier to the <code>domain</code>.
 */
public class SubdomainTenantResolver implements TenantResolver {

  private final Pattern validationPattern;
  private final int maxChars;

  public SubdomainTenantResolver(
      MultiTenancyConfig.TenantResolverConfig.SubdomainTenantResolverConfig config) {
    if (config.maxChars().isPresent()) {
      this.maxChars = config.maxChars().getAsInt();
    } else {
      this.maxChars = -1;
    }

    if (config.regex().isPresent()) {
      String regex = config.regex().get();
      this.validationPattern = Pattern.compile(regex);
    } else {
      this.validationPattern = null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Optional<String> resolve(RoutingContext context, SecurityContext securityContext) {
    // get host and first index of the dot
    String host = context.request().host();
    int index = host.indexOf('.');

    // if subdomain exists, take tenant id
    // otherwise empty
    if (index > 0) {
      String tenantId = host.substring(0, index);

      // if max chars is present
      // ensure subdomain is trimmed
      if (maxChars >= 0 && maxChars < tenantId.length()) {
        tenantId = tenantId.substring(0, maxChars);
      }

      // if regex defined check
      if (null != validationPattern) {
        boolean matches = validationPattern.matcher(tenantId).matches();
        if (!matches) {
          return Optional.empty();
        }
      }

      return Optional.of(tenantId);
    } else {
      return Optional.empty();
    }
  }
}
