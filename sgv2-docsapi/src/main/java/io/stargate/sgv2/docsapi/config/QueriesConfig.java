package io.stargate.sgv2.docsapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.bridge.proto.QueryOuterClass;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/** Queries configuration. */
@ConfigMapping(prefix = "stargate.queries")
public interface QueriesConfig {

  /** @return Settings for the consistency level. */
  @Valid
  QueriesConfig.ConsistencyConfig consistency();

  interface ConsistencyConfig {

    /** @return Consistency for queries making schema changes. */
    @WithDefault("LOCAL_QUORUM")
    @NotNull
    QueryOuterClass.Consistency schemaChanges();

    /** @return Consistency for queries writing the data. */
    @WithDefault("LOCAL_QUORUM")
    @NotNull
    QueryOuterClass.Consistency writes();

    /** @return Consistency for queries reading the data. */
    @WithDefault("LOCAL_QUORUM")
    @NotNull
    QueryOuterClass.Consistency reads();
  }
}
