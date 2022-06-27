package io.stargate.sgv2.docsapi.service.json;

import io.stargate.sgv2.docsapi.config.constants.Constants;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public interface DeadLeaf {
  DeadLeaf ARRAY_LEAF = ImmutableDeadLeaf.builder().name(Constants.GLOB_ARRAY_VALUE).build();
  DeadLeaf STAR_LEAF = ImmutableDeadLeaf.builder().name(Constants.GLOB_VALUE).build();

  String getName();
}
