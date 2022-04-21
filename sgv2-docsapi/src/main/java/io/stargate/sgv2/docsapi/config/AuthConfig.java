package io.stargate.sgv2.docsapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.docsapi.config.constants.Constants;
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
    boolean enabled();

    /**
     * @return Name of the authentication header. Defaults to {@value
     *     Constants#AUTHENTICATION_TOKEN_HEADER_NAME}.
     */
    @NotBlank
    @WithDefault(Constants.AUTHENTICATION_TOKEN_HEADER_NAME)
    String headerName();
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
     *       io.stargate.sgv2.docsapi.api.common.token.impl.HeaderTokenResolver}}
     *   <li><code>principal</code> - reads Cassandra token from the security {@link
     *       java.security.Principal} name (see {@link
     *       io.stargate.sgv2.docsapi.api.common.token.impl.PrincipalTokenResolver}}
     *   <li><code>fixed</code> - fixed token supplied by the configuration (see {@link
     *       io.stargate.sgv2.docsapi.api.common.token.impl.FixedTokenResolver}}
     *   <li><code>custom</code> - allows configuring custom token resolver
     * </ol>
     *
     * If unset, noop resolver will be used.
     *
     * @return The type of the {@link
     *     io.stargate.sgv2.docsapi.api.common.token.CassandraTokenResolver} used.
     */
    Optional<@Pattern(regexp = "header|principal|fixed|custom") String> type();

    /** @return Specific settings for the <code>header</code> token resolver type. */
    @Valid
    AuthConfig.TokenResolverConfig.HeaderTokenResolverConfig header();

    interface HeaderTokenResolverConfig {

      /**
       * @return Name of the header to read the Cassandra token. Defaults to {@value
       *     Constants#AUTHENTICATION_TOKEN_HEADER_NAME}.
       */
      @NotBlank
      @WithDefault(Constants.AUTHENTICATION_TOKEN_HEADER_NAME)
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
