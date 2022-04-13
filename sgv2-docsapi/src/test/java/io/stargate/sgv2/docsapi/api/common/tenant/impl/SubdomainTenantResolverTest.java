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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.stargate.sgv2.docsapi.api.common.tenant.TenantResolver;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
class SubdomainTenantResolverTest {

  @Inject // enabled by default
  Instance<TenantResolver> tenantResolver;

  @InjectMock(returnsDeepMocks = true)
  RoutingContext routingContext;

  @Nested
  class Resolve {

    @Test
    public void happyPath() {
      when(routingContext.request().host()).thenReturn("xyz.domain.host");

      Optional<String> result = tenantResolver.get().resolve(routingContext, null);

      assertThat(result).contains("xyz");
    }

    @Test
    public void topLevelDomain() {
      when(routingContext.request().host()).thenReturn("domain.host");

      Optional<String> result = tenantResolver.get().resolve(routingContext, null);

      assertThat(result).contains("domain");
    }

    @Test
    public void notDomain() {
      when(routingContext.request().host()).thenReturn("localhost");

      Optional<String> result = tenantResolver.get().resolve(routingContext, null);

      assertThat(result).isEmpty();
    }
  }
}
