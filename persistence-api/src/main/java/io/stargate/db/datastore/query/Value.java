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
