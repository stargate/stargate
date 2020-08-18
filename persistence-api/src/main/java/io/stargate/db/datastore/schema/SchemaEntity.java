/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.schema;

import java.io.Serializable;

import org.immutables.value.Value;

public interface SchemaEntity extends Serializable
{
    String name();

    @Value.Lazy
    default String cqlName()
    {
        return ColumnUtils.maybeQuote(name());
    }
}
