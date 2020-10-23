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
package io.stargate.api.sql.plan.exec;

import io.stargate.api.sql.schema.TypeUtils;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.linq4j.Enumerator;

public class ResultSetEnumerator implements Enumerator<Object> {
  private final List<Column> columns;
  private final CompletableFuture<ResultSet> futureResult;
  private Iterator<Row> resultSet;
  private Object current;

  public ResultSetEnumerator(List<Column> columns, CompletableFuture<ResultSet> futureResult) {
    this.columns = columns;
    this.futureResult = futureResult;
  }

  private Iterator<Row> iterator() {
    if (resultSet == null) {
      try {
        resultSet = futureResult.get().iterator();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return resultSet;
  }

  @Override
  public Object current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    if (!iterator().hasNext()) {
      return false;
    }

    Row row = iterator().next();

    current = buildRowObject(row, columns);
    return true;
  }

  private static Object buildRowObject(Row row, List<Column> columns) {
    if (columns.size() == 1) {
      return getValue(row, columns.get(0));
    } else {
      Object[] array = new Object[columns.size()];

      int idx = 0;
      for (Column column : columns) {
        array[idx++] = getValue(row, column);
      }

      return array;
    }
  }

  private static Object getValue(Row row, Column column) {
    Object rawValue = row.getObject(column.name());
    return TypeUtils.toCalciteValue(rawValue, Objects.requireNonNull(column.type()));
  }

  @Override
  public void close() {
    // nop
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }
}
