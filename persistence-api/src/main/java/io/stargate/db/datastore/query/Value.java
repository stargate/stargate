/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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

import io.stargate.db.schema.Column;
import org.immutables.value.Value.Style.ImplementationVisibility;

@org.immutables.value.Value.Immutable
@org.immutables.value.Value.Style(visibility = ImplementationVisibility.PACKAGE)
public abstract class Value<T> implements Parameter<T> {

  /**
   * Object that represents a value that is set, but to (CQL) null.
   *
   * <p>Note that a {@link Value} object cannot have a {@code null} (in the sense of java) {@link
   * #value()}, because {@link java.util.Optional} cannot have a {@code null} and an empty optional
   * represents an unbound value, <b>not</b> a null CQL value. So this object allows to represent a
   * null CQL value.
   *
   * <p>Note that you should never have to pass this object directly. Instead, simply pass {@code
   * null} to the {@link #create(Column, Object)} method. However, the create value will use this
   * "null" object underneath, so this exposed so it possible to test if the value of a {@link
   * Value} object is null (meaning, something like {@code if (v.value().isPresent() &&
   * v.value().get() == NULL)) ...}).
   */
  public static final Object NULL =
      new Object() {
        @Override
        public String toString() {
          return "<null>";
        }
      };

  /**
   * Creates a bound value for the provided column, bound to the provided value.
   *
   * @param c the column name.
   * @param value the value, which <b>can</b> be {@code null} (in which case the {@link #value()}
   *     method of the created value will return an non-empty optional with the {@link Value#NULL})
   *     value. Use {@link #createUnbound(Column)} if instead you want an unbound value.
   * @return the created value.
   */
  public static <V> Value<V> create(Column c, V value) {
    return ImmutableValue.<V>builder().column(c).value(value == null ? NULL : value).build();
  }

  /**
   * Creates a bound value for the provided column, bound to the provided value.
   *
   * @param c the column name.
   * @param value the value, which <b>can</b> be {@code null} (in which case the {@link #value()}
   *     method of the created value will return an non-empty optional with the {@link Value#NULL})
   *     value. Use {@link #createUnbound(String)} if instead you want an unbound value.
   * @return the created value.
   */
  public static <V> Value<V> create(String c, V value) {
    return create(Column.reference(c), value);
  }

  /**
   * Creates an unbound value for the provided column.
   *
   * @param c the column name.
   * @return the created (unbound) value.
   */
  public static <V> Value<V> createUnbound(Column c) {
    return ImmutableValue.<V>builder().column(c).build();
  }

  /**
   * Creates an unbound value for the provided column.
   *
   * @param c the column name.
   * @return the created (unbound) value.
   */
  public static <V> Value<V> createUnbound(String c) {
    return createUnbound(c);
  }
}
