/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.schema;

import java.io.Serializable;

import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class CollectionIndexingType implements Serializable
{
    @Value.Default
    public boolean indexKeys()
    {
        return false;
    }

    @Value.Default
    public boolean indexValues()
    {
        return false;
    }

    @Value.Default
    public boolean indexEntries()
    {
        return false;
    }

    @Value.Default
    public boolean indexFull()
    {
        return false;
    }
}
