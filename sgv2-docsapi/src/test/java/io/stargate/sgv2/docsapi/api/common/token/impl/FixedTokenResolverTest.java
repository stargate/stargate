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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.docsapi.api.common.token.CassandraTokenResolver;
import io.stargate.sgv2.docsapi.testprofiles.FixedTokenTestProfile;
import org.junit.jupiter.api.Test;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@TestProfile(FixedTokenTestProfile.class)
class FixedTokenResolverTest {

    @Inject
    Instance<CassandraTokenResolver> tokenResolver;

    // TODO move to @Nested with Quarkus fix
    //  https://github.com/quarkusio/quarkus/issues/24910

    @Test
    public void happyPath() {
        Optional<String> result = tokenResolver.get().resolve(null, null);

        assertThat(result).contains(FixedTokenTestProfile.TOKEN);
    }

}