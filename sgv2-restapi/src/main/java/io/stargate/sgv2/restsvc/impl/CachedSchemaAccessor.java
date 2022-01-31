package io.stargate.sgv2.restsvc.impl;

import io.stargate.proto.Schema;
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

  public Schema.CqlTable findTable(String keyspaceName, String tableName) {
    // Note: this can throw IllegalArgumentException
    final Schema.CqlKeyspaceDescribe keyspaceDef = schemaCache.getKeyspace(keyspaceName);
    for (int i = 0, end = keyspaceDef.getTablesCount(); i < end; ++i) {
      Schema.CqlTable table = keyspaceDef.getTables(i);
      if (tableName.equalsIgnoreCase(table.getName())) {
        return table;
      }
    }
    return null;
  }
}
