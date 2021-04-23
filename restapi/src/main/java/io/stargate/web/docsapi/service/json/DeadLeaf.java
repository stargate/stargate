package io.stargate.web.docsapi.service.json;

import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public interface DeadLeaf {
  String STAR = "*";
  String ARRAY = "[*]";

  String getName();
}
