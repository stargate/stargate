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

package io.stargate.sgv2.docsapi.api.common.token.impl;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.stargate.sgv2.docsapi.api.common.token.CassandraTokenResolver;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@QuarkusTest
@TestProfile(HeaderTokenResolverTest.Profile.class)
class HeaderTokenResolverTest {

    public static class Profile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return ImmutableMap.<String, String>builder()
                    .put("stargate.token-resolver.type", "header")
                    .put("stargate.token-resolver.header.header-name", "x-some-header")
                    .build();
        }
    }

    // TODO move to @Nested with Quarkus update to 2.8.0

    @Inject
    Instance<CassandraTokenResolver> tokenResolver;

    @InjectMock(returnsDeepMocks = true)
    RoutingContext routingContext;

    @Test
    public void happyPath() {
        String token = RandomStringUtils.randomAlphanumeric(16);
        when(routingContext.request().getHeader("x-some-header")).thenReturn(token);

        Optional<String> result = tokenResolver.get().resolve(routingContext, null);

        assertThat(result).contains(token);
    }

    @Test
    public void noHeader() {
        when(routingContext.request().getHeader("x-some-header")).thenReturn(null);

        Optional<String> result = tokenResolver.get().resolve(routingContext, null);

        assertThat(result).isEmpty();
    }

}