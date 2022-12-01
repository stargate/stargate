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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.stargate.sgv2.api.common.tenant.TenantResolver;
import io.vertx.ext.web.RoutingContext;
import java.util.Map;
import java.util.Optional;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(SubdomainTenantResolverWithRegexTest.Profile.class)
class SubdomainTenantResolverWithRegexTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.multi-tenancy.enabled", "true")
          .put("stargate.multi-tenancy.tenant-resolver.type", "subdomain")
          .put("stargate.multi-tenancy.tenant-resolver.subdomain.max-chars", "36")
          .put(
              "stargate.multi-tenancy.tenant-resolver.subdomain.regex",
              "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")
          .build();
    }
  }

  @Inject // enabled by default
  Instance<TenantResolver> tenantResolver;

  @InjectMock(returnsDeepMocks = true)
  RoutingContext routingContext;

  @Nested
  class Resolve {

    @Test
    public void happyPath() {
      when(routingContext.request().host())
          .thenReturn("09cedbf6-9086-42bb-93ac-e497682227ba-eu-west-1.domain.host");

      Optional<String> result = tenantResolver.get().resolve(routingContext, null);

      assertThat(result).contains("09cedbf6-9086-42bb-93ac-e497682227ba");
    }

    @Test
    public void tooShort() {
      when(routingContext.request().host())
          .thenReturn("09cedbf6-9086-42bb-93ac-e497682227b.domain.host");

      Optional<String> result = tenantResolver.get().resolve(routingContext, null);

      assertThat(result).isEmpty();
    }

    @Test
    public void regexNotMatched() {
      when(routingContext.request().host()).thenReturn("xyz.domain.host");

      Optional<String> result = tenantResolver.get().resolve(routingContext, null);

      assertThat(result).isEmpty();
    }
  }
}
