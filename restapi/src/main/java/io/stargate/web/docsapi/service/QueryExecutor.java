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
package io.stargate.web.docsapi.service;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.stargate.db.PagingPosition;
import io.stargate.db.PagingPosition.ResumeMode;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class QueryExecutor {
  private final Accumulator TERM = new Accumulator();

  private final DataStore dataStore;
  private final Table table;

  public QueryExecutor(DataStore dataStore, Table table) {
    this.dataStore = dataStore;
    this.table = table;
  }

  public Flowable<RawDocument> queryDocs(
      AbstractBound<?> query, int limit, ByteBuffer pagingState) {
    return execute(query, pagingState)
        .concatMap(rs -> Flowable.fromIterable(seeds(rs)))
        .concatWith(Single.just(TERM))
        .scan(Accumulator::combine)
        .filter(Accumulator::isComplete)
        .limit(limit)
        .map(Accumulator::toDoc);
  }

  public Flowable<ResultSet> execute(AbstractBound<?> query, ByteBuffer pagingState) {
    return executeSingle(query, pagingState).concatMap(rs -> fetchNext(rs, query));
  }

  private Flowable<ResultSet> executeSingle(AbstractBound<?> query, ByteBuffer pagingState) {
    return RxUtils.toSingle(
            dataStore.execute(query, p -> pagingState == null ? p : p.withPagingState(pagingState)))
        .toFlowable();
  }

  private Flowable<ResultSet> fetchNext(ResultSet rs, AbstractBound<?> query) {
    ByteBuffer nextPagingState = rs.getPagingState();
    if (nextPagingState == null) {
      return Flowable.just(rs);
    } else {
      return Flowable.just(rs)
          .concatWith(
              Flowable.just(nextPagingState)
                  .concatMap(ps -> executeSingle(query, ps).concatMap(r -> fetchNext(r, query))));
    }
  }

  private Iterable<Accumulator> seeds(ResultSet rs) {
    List<Row> rows = rs.currentPageRows();
    List<Accumulator> seeds = new ArrayList<>(rows.size());
    for (Row row : rows) {
      seeds.add(new Accumulator(rs, row));
    }
    return seeds;
  }

  public class Accumulator {

    private final String id;
    private final List<Row> rows;
    private final boolean complete;
    private final Accumulator next;
    private ResultSet lastResultSet;

    private Accumulator() {
      id = null;
      rows = null;
      next = null;
      complete = false;
    }

    private Accumulator(ResultSet resultSet, Row seedRow) {
      this.id = seedRow.getString("key");
      this.rows = new ArrayList<>();
      this.next = null;
      this.complete = false;
      this.lastResultSet = resultSet;

      rows.add(seedRow);
    }

    private Accumulator(String id, ResultSet resultSet, List<Row> rows, Accumulator next) {
      this.id = id;
      this.rows = rows;
      this.next = next;
      this.complete = true;
      this.lastResultSet = resultSet;
    }

    boolean isComplete() {
      return complete;
    }

    public RawDocument toDoc() {
      if (!complete) {
        throw new IllegalStateException("Incomplete document.");
      }

      return new RawDocument(id, lastResultSet, rows);
    }

    private Accumulator end() {
      if (next != null) {
        if (!complete) {
          throw new IllegalStateException("Ending an incomplete document");
        }

        return next.end();
      }

      if (complete) {
        throw new IllegalStateException("Already complete");
      }

      return new Accumulator(id, lastResultSet, rows, null);
    }

    private void append(Accumulator other) {
      rows.addAll(other.rows);
      lastResultSet = other.lastResultSet;
    }

    private Accumulator combine(Accumulator buffer) {
      if (buffer == TERM) {
        return end();
      }

      if (complete) {
        if (next == null) {
          throw new IllegalStateException(
              "Unexpected continuation after a terminal document element.");
        }

        return next.combine(buffer);
      }

      if (id.equals(buffer.id)) {
        append(buffer);
        return this; // still not complete
      } else {
        return new Accumulator(id, lastResultSet, rows, buffer);
      }
    }
  }

  public class RawDocument {

    private final String id;
    private final ResultSet resultSet;
    private final List<Row> rows;

    public RawDocument(String id, ResultSet resultSet, List<Row> rows) {
      this.id = id;
      this.resultSet = resultSet;
      this.rows = rows;
    }

    public String id() {
      return id;
    }

    public ByteBuffer makePagingState() {
      if (rows.isEmpty()) {
        throw new IllegalStateException("Cannot resume paging from an empty document");
      }

      Row lastRow = rows.get(rows.size() - 1);
      Column keyColumn = table.column("key");
      ByteBuffer keyValue = lastRow.getBytesUnsafe("key");

      if (keyValue == null) {
        throw new IllegalStateException("Missing document key");
      }

      return resultSet.makePagingState(
          PagingPosition.builder()
              .putCurrentRow(keyColumn, keyValue)
              .resumeFrom(ResumeMode.NEXT_PARTITION)
              .build());
    }
  }
}
