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
package io.stargate.db.datastore;

import io.stargate.db.PagingPosition;
import io.stargate.db.RowDecorator;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

/**
 * A simple result set implementation that simply stores data in a List. Produces {@link
 * MapBackedRow} rows when iterating the results.
 */
public class ListBackedResultSet implements ResultSet {

  private final List<Column> columns;
  private final List<Row> currentPage;
  private final ValidatingPaginator paginator;

  public ListBackedResultSet(List<Column> columns, List<Row> data, ValidatingPaginator paginator) {
    this.columns = columns;
    this.currentPage = data;
    this.paginator = paginator;
  }

  public static ResultSet of(
      Table table, List<Map<String, Object>> data, ValidatingPaginator paginator) {
    data = paginator.filter(data);
    Set<String> columnNames =
        data.stream().flatMap(m -> m.keySet().stream()).collect(Collectors.toSet());
    List<Column> columns =
        columnNames.stream()
            .map(table::column)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (columnNames.isEmpty()) { // empty `data`
      columns = table.columns(); // use all columns as a fallback for tests
    }

    List<Row> rows = data.stream().map(m -> MapBackedRow.of(table, m)).collect(Collectors.toList());
    return new ListBackedResultSet(columns, rows, paginator);
  }

  @Override
  public List<Column> columns() {
    return columns;
  }

  @Override
  public List<Row> currentPageRows() {
    return currentPage;
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
    throw new UnsupportedOperationException("Expecting page-by-page fetching");
  }

  @Override
  public Row one() {
    throw new UnsupportedOperationException("Expecting page-by-page fetching");
  }

  @Override
  public List<Row> rows() {
    throw new UnsupportedOperationException("Expecting page-by-page fetching");
  }

  @Override
  public ByteBuffer getPagingState() {
    return paginator.pagingState();
  }

  @Override
  public ByteBuffer makePagingState(PagingPosition position) {
    return paginator.pagingState(position, currentPage);
  }

  @Override
  public RowDecorator makeRowDecorator() {
    return new AlphabeticalOrderPartitionKeyDecorator(columns);
  }
}
