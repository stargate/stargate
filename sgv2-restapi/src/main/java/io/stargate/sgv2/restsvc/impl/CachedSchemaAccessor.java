package io.stargate.sgv2.restsvc.impl;

import io.stargate.sgv2.common.schema.SchemaCache;

/**
 * Accessor class that will abstract out the details of getting information and handlers for CQL
 * Tables through streaming Schema updates.
 */
public class CachedSchemaAccessor {
  private final SchemaCache schemaCache;

  private CachedSchemaAccessor(SchemaCache cache) {
    schemaCache = cache;
  }

  public static CachedSchemaAccessor construct(SchemaCache cache) {
    return new CachedSchemaAccessor(cache);
  }

  // !!! TODO: Actual access
}
