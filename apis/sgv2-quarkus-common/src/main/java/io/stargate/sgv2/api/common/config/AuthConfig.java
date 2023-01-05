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
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.token.CassandraTokenResolver;
import io.stargate.sgv2.api.common.token.impl.FixedTokenResolver;
import io.stargate.sgv2.api.common.token.impl.HeaderTokenResolver;
import io.stargate.sgv2.api.common.token.impl.PrincipalTokenResolver;
import java.util.Optional;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

/** Authentication configuration. */
@ConfigMapping(prefix = "stargate.auth")
public interface AuthConfig {

  /** @return Header based authentication setup. */
  @Valid
  HeaderBasedAuthConfig headerBased();

  /** Configuration for the header based authentication. */
  interface HeaderBasedAuthConfig {

    /** @return If the header based auth is enabled. */
    @WithDefault("true")
    boolean enabled();

    /**
     * @return Name of the authentication header. Defaults to {@value
     *     HttpConstants#AUTHENTICATION_TOKEN_HEADER_NAME}.
     */
    @NotBlank
    @WithDefault(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME)
    String headerName();

    /** @return If the customization of the challenge sending should be done. */
    @WithDefault("${stargate.exception-mappers.enabled:true}")
    boolean customChallengeEnabled();
  }

  /** @return Configuration for the cassandra token resolver. */
  @Valid
  TokenResolverConfig tokenResolver();

  /** Configuration mapping for the token resolver. */
  interface TokenResolverConfig {

    /**
     * Cassandra token resolver type, possible options:
     *
     * <ol>
     *   <li><code>header</code> - reads Cassandra token from the HTTP request header (see {@link
     *       HeaderTokenResolver}}
     *   <li><code>principal</code> - reads Cassandra token from the security {@link
     *       java.security.Principal} name (see {@link PrincipalTokenResolver}}
     *   <li><code>fixed</code> - fixed token supplied by the configuration (see {@link
     *       FixedTokenResolver}}
     *   <li><code>custom</code> - allows configuring custom token resolver
     * </ol>
     *
     * If unset, noop resolver will be used.
     *
     * @return The type of the {@link CassandraTokenResolver} used.
     */
    @WithDefault("principal")
    Optional<@Pattern(regexp = "header|principal|fixed|custom") String> type();

    /** @return Specific settings for the <code>header</code> token resolver type. */
    @Valid
    AuthConfig.TokenResolverConfig.HeaderTokenResolverConfig header();

    interface HeaderTokenResolverConfig {

      /**
       * @return Name of the header to read the Cassandra token. Defaults to {@value
       *     HttpConstants#AUTHENTICATION_TOKEN_HEADER_NAME}.
       */
      @NotBlank
      @WithDefault(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME)
      String headerName();
    }

    /** @return Specific settings for the <code>fixed</code> token resolver type. */
    @Valid
    AuthConfig.TokenResolverConfig.FixedTokenResolverConfig fixed();

    interface FixedTokenResolverConfig {

      /** @return Token value. */
      Optional<String> token();
    }
  }
}
