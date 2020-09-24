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

import io.stargate.db.datastore.schema.Table;
import java.util.List;
import java.util.Objects;

public class UnsupportedQueryException extends IllegalArgumentException {
  private String cql;
  private final transient Table table;
  private final transient Where<?> where;
  private final transient List<ColumnOrder> orders;

  public UnsupportedQueryException(
      String cql, Table table, Where<?> where, List<ColumnOrder> orders) {
    super(String.format("No table or view could satisfy the query '%s'", cql));
    this.cql = cql;
    this.table = table;
    this.where = where;
    this.orders = orders;
  }

  public String getCql() {
    return cql;
  }

  public Table getTable() {
    return table;
  }

  public Where<?> getWhere() {
    return where;
  }

  public List<ColumnOrder> getOrders() {
    return orders;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UnsupportedQueryException that = (UnsupportedQueryException) o;
    return Objects.equals(cql, that.cql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cql);
  }
}
