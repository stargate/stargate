package io.stargate.web.docsapi.service.json;

import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public interface DeadLeaf {
  String STAR = "*";
  String ARRAY = "[*]";
  DeadLeaf ARRAYLEAF = ImmutableDeadLeaf.builder().name(DeadLeaf.ARRAY).build();
  DeadLeaf STARLEAF = ImmutableDeadLeaf.builder().name(DeadLeaf.STAR).build();

  String getName();
}
