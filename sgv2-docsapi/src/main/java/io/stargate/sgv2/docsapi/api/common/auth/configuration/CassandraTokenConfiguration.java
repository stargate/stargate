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

package io.stargate.sgv2.docsapi.api.common.auth.configuration;

import io.quarkus.arc.DefaultBean;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.stargate.sgv2.docsapi.api.common.auth.CassandraTokenResolver;
import io.stargate.sgv2.docsapi.api.common.auth.impl.HeaderTokenResolver;
import io.stargate.sgv2.docsapi.config.StargateConfig;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.Optional;

/**
 * Configuration for activating a correct {@link CassandraTokenResolver}.
 */
public class CassandraTokenConfiguration {

    @Produces
    @ApplicationScoped
    @LookupIfProperty(
            name = "stargate.token-resolver.type",
            stringValue = "header"
    )
    CassandraTokenResolver headerCassandraTokenResolver(StargateConfig stargateConfig) {
        String headerName = stargateConfig.tokenResolver().header().headerName();
        return new HeaderTokenResolver(headerName);
    }

    @Produces
    @ApplicationScoped
    @DefaultBean
    CassandraTokenResolver noopCassandraTokenResolver() {
        return context -> Optional.empty();
    }

}
