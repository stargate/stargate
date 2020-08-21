/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.schema;

import org.immutables.value.Value;

public interface QualifiedSchemaEntity extends SchemaEntity
{
    String keyspace();

    @Value.Lazy
    default String cqlKeyspace()
    {
        return ColumnUtils.maybeQuote(keyspace());
    }
}
