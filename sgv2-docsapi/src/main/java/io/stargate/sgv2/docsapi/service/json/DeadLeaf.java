package io.stargate.sgv2.docsapi.service.json;

import io.stargate.sgv2.docsapi.config.constants.Constants;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public interface DeadLeaf {
  DeadLeaf ARRAYLEAF = ImmutableDeadLeaf.builder().name(Constants.GLOB_ARRAY_VALUE).build();
  DeadLeaf STARLEAF = ImmutableDeadLeaf.builder().name(Constants.GLOB_VALUE).build();

  String getName();
}
