/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
