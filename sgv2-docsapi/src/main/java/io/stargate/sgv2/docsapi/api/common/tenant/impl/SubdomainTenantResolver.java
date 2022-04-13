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

package io.stargate.sgv2.docsapi.api.common.tenant.impl;

import io.stargate.sgv2.docsapi.api.common.tenant.TenantResolver;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import javax.ws.rs.core.SecurityContext;

/**
 * {@link TenantResolver} that finds the tenant ID in the left most domain part of the host name.
 *
 * <p>For example, having <code>tenant-id.domain.com</code> will resolve tenant identifier to the
 * <code>tenant-id</code>. In case of top-level domain, <code>domain.com</code> will resolve tenant
 * identifier to the <code>domain</code>.
 */
public class SubdomainTenantResolver implements TenantResolver {

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
      return Optional.of(tenantId);
    } else {
      return Optional.empty();
    }
  }
}
