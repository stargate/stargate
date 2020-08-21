/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.query;

import java.util.stream.Collectors;

import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class AndWhere<T> implements NWhere<T>
{
    public static AndWhere and(Where... wheres)
    {
        return ImmutableAndWhere.builder().addChildren(wheres).build();
    }

    @Override
    public String toString()
    {
        return children().stream().map(c -> c.toString()).collect(Collectors.joining(" AND ", "(", ")"));
    }
}
