package io.stargate.web.docsapi.service.util;

import org.immutables.value.Value;

@Value.Immutable
public interface KeyspaceAndTable {
  String getKeyspace();

  String getTable();
}
