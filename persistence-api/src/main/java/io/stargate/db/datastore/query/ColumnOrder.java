/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.query;

import org.immutables.value.Value;

import io.stargate.db.datastore.schema.Column;

@Value.Immutable(prehash = true)
public abstract class ColumnOrder
{

    public abstract Column column();

    public abstract Column.Order order();

    public static ColumnOrder of(Column column, Column.Order order)
    {
        return ImmutableColumnOrder.builder().column(column).order(order).build();
    }

    public static ColumnOrder of(Column column)
    {
        return ImmutableColumnOrder.builder().column(column).order(Column.Order.Asc).build();
    }

    public static ColumnOrder of(String column, Column.Order order)
    {
        return ImmutableColumnOrder.builder().column(Column.reference(column)).order(order)
                .build();
    }

    public static ColumnOrder of(String column)
    {
        return ImmutableColumnOrder.builder().column(Column.reference(column))
                .order(Column.Order.Asc).build();
    }

    @Override
    public String toString()
    {
        return column().name() + " " + order();
    }
}
