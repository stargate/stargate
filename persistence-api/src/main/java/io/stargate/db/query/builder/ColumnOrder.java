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
package io.stargate.db.query.builder;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Order;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Value.Immutable(prehash = true)
@Value.Style(visibility = ImplementationVisibility.PACKAGE)
public abstract class ColumnOrder {

  public abstract String column();

  public abstract Column.Order order();

  public static ColumnOrder of(Column column, Column.Order order) {
    return of(column.name(), order);
  }

  public static ColumnOrder of(Column column) {
    return of(column.name());
  }

  public static ColumnOrder of(String column, Column.Order order) {
    return ImmutableColumnOrder.builder().column(column).order(order).build();
  }

  public static ColumnOrder of(String column) {
    return of(column, Order.ASC);
  }

  @Override
  public String toString() {
    return column() + " " + order();
  }
}
