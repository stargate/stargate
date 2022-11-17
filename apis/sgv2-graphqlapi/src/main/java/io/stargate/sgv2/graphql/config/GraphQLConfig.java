package io.stargate.sgv2.graphql.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.token.CassandraTokenResolver;
import java.util.Optional;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/** Configuration for the GraphQL. */
@ConfigMapping(prefix = "stargate.graphql")
public interface GraphQLConfig {

  /**
   * @return Whether to default to the oldest keyspace when the user accesses /graphql. If this is
   *     disabled, /graphql throws an error, and the keyspace must be provided explicitly in the
   *     path, as in /graphql/ksName.
   */
  @WithDefault("true")
  boolean enableDefaultKeyspace();

  /** @return Configuration for the GraphQL Playground. */
  @NotNull
  @Valid
  PlaygroundConfig playground();

  interface PlaygroundConfig {

    /** @return Whether to expose the GraphQL Playground at /playground. */
    @WithDefault("true")
    boolean enabled();

    /**
     * @return Optional, name of the HTTP header where to extract the token in case {@link
     *     CassandraTokenResolver} does not provide a token.
     */
    @WithDefault(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME)
    Optional<@NotBlank String> tokenHeader();
  }
}
