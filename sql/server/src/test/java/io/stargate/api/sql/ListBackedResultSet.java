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
package io.stargate.api.sql;

import com.google.common.collect.ImmutableList;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Table;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import javax.validation.constraints.NotNull;

/**
 * A simple result set implementation that simply stores data in a List. Produces {@link
 * MapBackedRow} rows when iterating the results.
 */
public class ListBackedResultSet implements ResultSet {
  private final Iterator<Row> iterator;

  public static ResultSet of(Table table, List<Map<String, Object>> data) {
    return new ListBackedResultSet(
        new Iterator<Row>() {
          private final Iterator<Map<String, Object>> dataIterator = data.iterator();

          @Override
          public boolean hasNext() {
            return dataIterator.hasNext();
          }

          @Override
          public Row next() {
            return MapBackedRow.of(table, dataIterator.next());
          }
        });
  }

  public ListBackedResultSet(Iterator<Row> iterator) {
    this.iterator = iterator;
  }

  @Override
  public List<Row> currentPageRows() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNoMoreFetchedRows() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSet withRowInspector(Predicate<Row> authzFilter) {
    throw new UnsupportedOperationException();
  }

  @Override
  @NotNull
  public Iterator<Row> iterator() {
    return iterator;
  }

  @Override
  public Row one() {
    return iterator.next();
  }

  @Override
  public List<Row> rows() {
    return ImmutableList.copyOf(iterator);
  }

  @Override
  public ByteBuffer getPagingState() {
    throw new UnsupportedOperationException();
  }
}
