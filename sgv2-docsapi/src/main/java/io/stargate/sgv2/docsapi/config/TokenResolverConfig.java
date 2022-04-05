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
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.docsapi.config.constants.Constants;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import java.util.Optional;

/**
 * Configuration mapping for the token resolver.
 */
@ConfigMapping(prefix = "stargate.token-resolver")
public interface TokenResolverConfig {

    /**
     * Cassandra token resolver type, possible options:
     * <ol>
     *     <li><code>header</code> - reads Cassandra token from the HTTP request header (see {@link io.stargate.sgv2.docsapi.api.common.token.impl.HeaderTokenResolver}}</li>
     *     <li><code>principal</code> - reads Cassandra token from the security {@link java.security.Principal} name (see {@link io.stargate.sgv2.docsapi.api.common.token.impl.PrincipalTokenResolver}}</li>
     *     <li><code>custom</code> - allows configuring custom token resolver</li>
     * </ol>
     * If unset, noop resolver will be used.
     *
     * @return The type of the {@link io.stargate.sgv2.docsapi.api.common.token.CassandraTokenResolver} used.
     */
    Optional<@Pattern(regexp = "header|principal|custom") String> type();

    /**
     * @return Specific settings for the <code>header</code> token resolver type.
     */
    @Valid
    HeaderTokenResolverConfig header();

    interface HeaderTokenResolverConfig {

        /**
         * @return Name of the header to read the Cassandra token. Defaults to {@value Constants#AUTHENTICATION_TOKEN_HEADER_NAME}.
         */
        @NotBlank
        @WithDefault(Constants.AUTHENTICATION_TOKEN_HEADER_NAME)
        String headerName();

    }

}
