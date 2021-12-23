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
package io.stargate.db.datastore;

import io.stargate.db.PagingPosition;
import io.stargate.db.RowDecorator;
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import javax.validation.constraints.NotNull;

public interface ResultSet extends Iterable<Row> {

  ResultSet withRowInspector(Predicate<Row> authzFilter);

  class Empty implements ResultSet {

    private final boolean waitedForSchemaAgreement;

    private Empty(boolean waitedForSchemaAgreement) {

      this.waitedForSchemaAgreement = waitedForSchemaAgreement;
    }

    @Override
    public ResultSet withRowInspector(Predicate<Row> authzFilter) {
      return this;
    }

    @Override
    public List<Column> columns() {
      return Collections.emptyList();
    }

    @Override
    public Iterator<Row> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public Row one() {
      throw new NoSuchElementException();
    }

    @Override
    public List<Row> rows() {
      return Collections.emptyList();
    }

    @Override
    public List<Row> currentPageRows() {
      return Collections.emptyList();
    }

    @Override
    public boolean hasNoMoreFetchedRows() {
      return true;
    }

    @Override
    public ByteBuffer getPagingState() {
      return null;
    }

    @Override
    public ByteBuffer makePagingState(PagingPosition position) {
      return null;
    }

    @Override
    public RowDecorator makeRowDecorator() {
      throw new UnsupportedOperationException(
          "Obtaining partition key comparators from an empty result set is not supported");
    }

    @Override
    public boolean waitedForSchemaAgreement() {
      return waitedForSchemaAgreement;
    }
  }

  ResultSet EMPTY_NO_SCHEMA_AGREEMENT = new Empty(false);
  ResultSet EMPTY_WITH_SCHEMA_AGREEMENT = new Empty(true);

  static ResultSet empty(boolean waitedForSchemaAgreement) {
    return waitedForSchemaAgreement ? EMPTY_WITH_SCHEMA_AGREEMENT : EMPTY_NO_SCHEMA_AGREEMENT;
  }

  static ResultSet empty() {
    return EMPTY_NO_SCHEMA_AGREEMENT;
  }

  List<Column> columns();

  @NotNull
  @Override
  Iterator<Row> iterator();

  /**
   * @return the next row in the current page. This is the same as calling the {@link #iterator()}}
   *     next method, and will attempt to fetch another page if the current page is exhausted.
   */
  Row one();

  /**
   * @return the remaining rows not yet iterated, in the current page or any other page not yet
   *     fetched. Use this method with care as it can potentially retrieve many pages and return a
   *     lot of data.
   */
  List<Row> rows();

  /** @return the rows of the currently fetched page. */
  List<Row> currentPageRows();

  /**
   * @return true if no more rows are available in the current page, without trying to fetch any
   *     additional pages.
   */
  boolean hasNoMoreFetchedRows();

  ByteBuffer getPagingState();

  /**
   * Creates a paging state from a custom paging position for fetching more data from the query that
   * returned this {@link ResultSet}.
   */
  ByteBuffer makePagingState(PagingPosition position);

  /**
   * Makes a new {@link RowDecorator} for the table providing columns in this results set.
   *
   * @throws IllegalArgumentException if none or more than one table is referenced by this result
   *     set.
   */
  RowDecorator makeRowDecorator();

  /** Returns true of this request waited for schema agreement. */
  default boolean waitedForSchemaAgreement() {
    return false;
  }
}
