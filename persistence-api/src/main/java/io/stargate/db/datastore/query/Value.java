/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.query;

import io.stargate.db.datastore.schema.Column;

@org.immutables.value.Value.Immutable
public abstract class Value<T> implements Parameter<T>
{
    public interface Builder<T>
    {

        default ImmutableValue.Builder<T> column(String column)
        {
            return column(Column.reference(column));
        }

        ImmutableValue.Builder<T> column(Column column);

    }

    public static <V> Value<V> create(String c, V value) {
        return ImmutableValue.<V>builder().column(c).value(value).build();
    }
}
