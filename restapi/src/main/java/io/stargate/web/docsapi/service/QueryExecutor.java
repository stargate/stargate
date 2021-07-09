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

import static io.stargate.web.docsapi.service.CombinedPagingState.EXHAUSTED_PAGE_STATE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import hu.akarnokd.rxjava3.operators.ExpandStrategy;
import hu.akarnokd.rxjava3.operators.FlowableTransformers;
import hu.akarnokd.rxjava3.operators.Flowables;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.stargate.db.ComparableKey;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.PagingPosition;
import io.stargate.db.PagingPosition.ResumeMode;
import io.stargate.db.RowDecorator;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.BuiltSelect;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.web.rx.RxUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/** Executes pre-built document queries, groups document rows and manages document pagination. */
public class QueryExecutor {

  private final Accumulator TERM = new Accumulator();

  private final DataStore dataStore;

  public QueryExecutor(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  public Flowable<RawDocument> queryDocs(
      List<BoundQuery> queries, int pageSize, ByteBuffer pagingState, ExecutionContext context) {
    return queryDocs(1, queries, pageSize, pagingState, context);
  }

  public Flowable<RawDocument> queryDocs(
      BoundQuery query, int pageSize, ByteBuffer pagingState, ExecutionContext context) {
    return queryDocs(1, ImmutableList.of(query), pageSize, pagingState, context);
  }

  public Flowable<RawDocument> queryDocs(
      int keyDepth,
      BoundQuery query,
      int pageSize,
      ByteBuffer pagingState,
      ExecutionContext context) {
    return queryDocs(keyDepth, ImmutableList.of(query), pageSize, pagingState, context);
  }

  /**
   * Runs the provided queries in parallel and merges their result sets according to the selected
   * primary key columns, then groups the merged rows into {@link RawDocument} objects according to
   * {@code keyDepth} primary key values.
   *
   * <p>Note: all queries must select the same set of columns from the same table.
   *
   * <p>Note: queries may use different {@code WHERE} conditions.
   *
   * <p>Note: the queries must select all the partition key columns and zero or more clustering key
   * columns with the total number of selected key columns equal to the {@code keyDepth} parameter.
   *
   * <p>Rows having the same set of value in their selected primary key columns are considered
   * duplicates and only one of them (in no particular order) will be use in the output of this
   * method.
   *
   * @param keyDepth the number of primary key columns to use for distinguishing documents.
   * @param queries the queries to run (one or more).
   * @param pageSize the storage-level page size to use (1 or greater).
   * @param pagingState the storage-level page state to use (may be {@code null}).
   * @param context the query execution context for profiling.
   * @return a flow of documents grouped by the primary key values up to {@code keyDepth} and
   *     ordered according to the natural Cassandra result set order (ring order on the partition
   *     key and default order on clustering keys).
   */
  public Flowable<RawDocument> queryDocs(
      int keyDepth,
      List<BoundQuery> queries,
      int pageSize,
      ByteBuffer pagingState,
      ExecutionContext context) {
    if (pageSize <= 0) {
      // Note: if page size is not set, C* will ignore paging state, but subsequent pages may be
      // requested by the execute() method, so we require a specific page size for all queries here.
      throw new IllegalArgumentException("Unsupported page size: " + pageSize);
    }

    QueryData queryData = ImmutableQueryData.builder().queries(queries).build();

    List<Column> idColumns = queryData.docIdColumns(keyDepth);
    Comparator<DocProperty> comparator = rowComparator(queryData);

    List<ByteBuffer> pagingStates = CombinedPagingState.deserialize(queries.size(), pagingState);

    PagingStateTracker tracker = new PagingStateTracker(pagingStates);

    return execute(queries, comparator, pageSize, pagingStates, context)
        .map(p -> toSeed(p, comparator, idColumns))
        .concatWith(Single.just(TERM))
        .scan(tracker::combine)
        .filter(Accumulator::isComplete)
        .map(Accumulator::toDoc);
  }

  /**
   * Builds a comparator that follows the natural order of rows in document tables, but limited to
   * the selected clustering columns (i.e., "path" columns).
   */
  private Comparator<DocProperty> rowComparator(QueryData queries) {
    List<Comparator<DocProperty>> comparators = new ArrayList<>();

    comparators.add(Comparator.comparing(DocProperty::comparableKey));

    for (Column column : queries.docPathColumns()) {
      comparators.add(Comparator.comparing(p -> p.keyValue(column)));
    }

    return (p1, p2) -> {
      // Avoid recursion because the number of "path" columns can be quite large.
      int result = 0;
      for (Comparator<DocProperty> comparator : comparators) {
        result = comparator.compare(p1, p2);
        if (result != 0) {
          return result;
        }
      }
      return result;
    };
  }

  private Flowable<DocProperty> execute(
      List<BoundQuery> queries,
      Comparator<DocProperty> comparator,
      int pageSize,
      List<ByteBuffer> pagingState,
      ExecutionContext context) {
    List<Flowable<DocProperty>> flows = new ArrayList<>(queries.size());
    int idx = 0;
    for (BoundQuery query : queries) {
      final int finalIdx = idx++;
      ByteBuffer queryPagingState = pagingState.get(finalIdx);
      if (queryPagingState != null) {
        queryPagingState = queryPagingState.slice();
      }

      flows.add(
          execute(query, pageSize, queryPagingState)
              .flatMap(
                  rs -> Flowable.fromIterable(properties(finalIdx, query, rs, context)),
                  1)); // max concurrency 1
    }

    return Flowables.orderedMerge(flows, comparator, false, 1); // prefetch 1
  }

  public Flowable<ResultSet> execute(BoundQuery query, int pageSize, ByteBuffer pagingState) {
    // An empty paging state means the query was exhaused during previous execution
    if (pagingState != null && pagingState.remaining() == 0) {
      return Flowable.empty();
    }

    return fetchPage(query, pageSize, pagingState)
        .compose( // Expand BREADTH_FIRST to reduce the number of "proactive" page requests
            FlowableTransformers.expand(
                rs -> fetchNext(rs, pageSize, query), ExpandStrategy.BREADTH_FIRST, 1));
  }

  private Flowable<ResultSet> fetchPage(BoundQuery query, int pageSize, ByteBuffer pagingState) {
    Supplier<CompletableFuture<ResultSet>> supplier =
        () ->
            dataStore.execute(
                query,
                p -> {
                  ImmutableParameters.Builder builder = p.toBuilder();
                  builder.pageSize(pageSize);
                  if (pagingState != null) {
                    builder.pagingState(pagingState);
                  }
                  return builder.build();
                });

    return RxUtils.singleFromFuture(supplier)
        .toFlowable()
        .compose(FlowableConnectOnRequest.with()) // separate subscription from query execution
        .take(1);
  }

  private Flowable<ResultSet> fetchNext(ResultSet rs, int pageSize, BoundQuery query) {
    ByteBuffer nextPagingState = rs.getPagingState();
    if (nextPagingState == null) {
      return Flowable.empty();
    } else {
      return fetchPage(query, pageSize, nextPagingState);
    }
  }

  private Accumulator toSeed(
      DocProperty property, Comparator<DocProperty> comparator, List<Column> keyColumns) {
    Row row = property.row();
    String id = row.getString("key");
    Builder<String> docKey = ImmutableList.builder();
    for (Column c : keyColumns) {
      docKey.add(Objects.requireNonNull(row.getString(c.name())));
    }

    return new Accumulator(id, comparator, docKey.build(), property);
  }

  /**
   * Converts a single page of results into {@link DocProperty} objects to maintain an association
   * of rows to their respective {@link ResultSet} objects and queries (the latter is needed for
   * tracking the combined paging state).
   */
  private Iterable<DocProperty> properties(
      int queryIndex, BoundQuery query, ResultSet rs, ExecutionContext context) {
    List<Row> rows = rs.currentPageRows();
    context.traceCqlResult(query, rows.size());

    Page page = ImmutablePage.builder().resultSet(rs).build();
    List<DocProperty> properties = new ArrayList<>(rows.size());
    int count = rows.size();
    for (Row row : rows) {
      boolean last = --count <= 0;
      properties.add(
          ImmutableDocProperty.builder()
              .queryIndex(queryIndex)
              .page(page)
              .row(row)
              .lastInPage(last)
              .build());
    }
    return properties;
  }

  public DataStore getDataStore() {
    return dataStore;
  }

  private class Accumulator {

    private final String id;
    private final Comparator<DocProperty> rowComparator;
    private final List<String> docKey;
    private final List<DocProperty> rows;
    private final List<PagingStateSupplier> pagingState;
    private final boolean complete;
    private final Accumulator next;

    private Accumulator() {
      id = null;
      rowComparator =
          Comparator.comparing(
              r -> {
                throw new IllegalStateException("Cannot append to the terminal element");
              });

      docKey = Collections.emptyList();
      rows = Collections.emptyList();
      pagingState = Collections.emptyList();
      next = null;
      complete = false;
    }

    private Accumulator(
        String id,
        Comparator<DocProperty> rowComparator,
        List<String> docKey,
        DocProperty seedRow) {
      this.id = id;
      this.rowComparator = rowComparator;
      this.docKey = docKey;
      this.rows = new ArrayList<>();
      this.pagingState = Collections.emptyList();
      this.next = null;
      this.complete = false;

      rows.add(seedRow);
    }

    private Accumulator(
        Accumulator complete,
        List<DocProperty> rows,
        List<PagingStateSupplier> pagingState,
        Accumulator next) {
      this.id = complete.id;
      this.rowComparator = complete.rowComparator;
      this.docKey = complete.docKey;
      this.rows = rows;
      this.pagingState = pagingState;
      this.next = next;
      this.complete = true;
    }

    boolean isComplete() {
      return complete;
    }

    public RawDocument toDoc() {
      if (!complete) {
        throw new IllegalStateException("Incomplete document.");
      }

      List<Row> docRows = this.rows.stream().map(DocProperty::row).collect(Collectors.toList());

      CombinedPagingState combinedPagingState = new CombinedPagingState(pagingState);

      return new RawDocument(id, docKey, combinedPagingState, docRows);
    }

    private Accumulator complete(PagingStateTracker tracker, Accumulator next) {
      // Deduplicate included rows and run them through the paging state tracker.
      // Note: the `rows` should already be in the order consistent with `rowComparator`.
      List<DocProperty> finalRows = new ArrayList<>(rows.size());
      DocProperty last = null;
      for (DocProperty row : rows) {
        tracker.track(row);

        if (last == null || rowComparator.compare(last, row) != 0) {
          finalRows.add(row);
        }

        last = row;
      }

      List<PagingStateSupplier> currentPagingState = tracker.slice();

      return new Accumulator(this, finalRows, currentPagingState, next);
    }

    private Accumulator end(PagingStateTracker tracker) {
      if (next != null) {
        if (!complete) {
          throw new IllegalStateException("Ending an incomplete document");
        }

        return next.end(tracker);
      }

      if (complete) {
        throw new IllegalStateException("Already complete");
      }

      return complete(tracker, null);
    }

    private void append(Accumulator other) {
      rows.addAll(other.rows);
    }

    private Accumulator combine(PagingStateTracker tracker, Accumulator buffer) {
      if (buffer == TERM) {
        return end(tracker);
      }

      if (complete) {
        if (next == null) {
          throw new IllegalStateException(
              "Unexpected continuation after a terminal document element.");
        }

        return next.combine(tracker, buffer);
      }

      if (docKey.equals(buffer.docKey)) {
        append(buffer);
        return this; // still not complete
      } else {
        return complete(tracker, buffer);
      }
    }
  }

  private static class PagingStateTracker {
    private final ArrayList<PagingStateSupplier> states;

    private PagingStateTracker(List<ByteBuffer> initialStates) {
      states = new ArrayList<>(initialStates.size());
      initialStates.stream().map(PagingStateSupplier::fixed).forEach(states::add);
    }

    private Accumulator combine(Accumulator prev, Accumulator next) {
      return prev.combine(this, next);
    }

    public void track(DocProperty row) {
      states.set(row.queryIndex(), row);
    }

    public List<PagingStateSupplier> slice() {
      return new ArrayList<>(states);
    }
  }

  /**
   * Represents individual rows that provide values for document properties.
   *
   * <p>This class links a {@link Row} to its source {@link Page} and the {@link #queryIndex()
   * query} that produced it.
   */
  @Value.Immutable(lazyhash = true)
  public abstract static class DocProperty implements PagingStateSupplier {

    abstract Page page();

    abstract Row row();

    /** Indicates whether the associated row was the last row in its page. */
    abstract boolean lastInPage();

    /**
     * The index of the query that produced this row in the list of executed queries.
     *
     * <p>This index corresponds to the position of the query's paging state in the combined paging
     * state.
     */
    abstract int queryIndex();

    @Value.Lazy
    ComparableKey<?> comparableKey() {
      return page().decorator().decoratePartitionKey(row());
    }

    String keyValue(Column column) {
      return row().getString(column.name());
    }

    @Override
    public ByteBuffer makePagingState(ResumeMode resumeMode) {
      // Note: is current row was not the last one in its page the higher-level request may
      // choose to resume fetching from the next row in current page even if the page itself does
      // not have a paging state. Therefore, in this case we cannot return EXHAUSTED_PAGE_STATE.
      if (lastInPage() && page().resultSet().getPagingState() == null) {
        return EXHAUSTED_PAGE_STATE;
      }

      return page()
          .resultSet()
          .makePagingState(PagingPosition.ofCurrentRow(row()).resumeFrom(resumeMode).build());
    }

    @Value.Lazy
    @Override
    public String toString() {
      return row().toString();
    }
  }

  /** A thin wrapper around {@link ResultSet} that caches {@link #decorator()} objects. */
  @Value.Immutable(lazyhash = true)
  public abstract static class Page {

    abstract ResultSet resultSet();

    /** Lazily initialized and cached {@link RowDecorator}. */
    @Value.Lazy
    RowDecorator decorator() {
      return resultSet().makeRowDecorator();
    }
  }

  /** A helper class for dealing with a list of related queries. */
  @Value.Immutable(lazyhash = true)
  public abstract static class QueryData {

    abstract List<BoundQuery> queries();

    @Value.Lazy
    List<BuiltSelect> selectQueries() {
      return queries().stream()
          .map(q -> (BuiltSelect) q.source().query())
          .collect(Collectors.toList());
    }

    @Value.Lazy
    Table table() {
      Set<Table> tables =
          selectQueries().stream().map(BuiltSelect::table).collect(Collectors.toSet());

      if (tables.isEmpty()) {
        throw new IllegalArgumentException("No tables are referenced by the provided queries");
      }

      if (tables.size() > 1) {
        throw new IllegalArgumentException(
            "Too many tables are referenced by the provided queries: "
                + tables.stream().map(Table::name).collect(Collectors.joining(", ")));
      }

      return tables.iterator().next();
    }

    @Value.Lazy
    Set<Column> selectedColumns() {
      Set<Set<Column>> sets =
          selectQueries().stream().map(BuiltSelect::selectedColumns).collect(Collectors.toSet());

      if (sets.size() != 1) {
        throw new IllegalArgumentException(
            "Incompatible sets of columns are selected by the provided queries: "
                + sets.stream()
                    .map(
                        s ->
                            s.stream().map(Column::name).collect(Collectors.joining(",", "[", "]")))
                    .collect(Collectors.joining("; ", "[", "]")));
      }

      return sets.iterator().next();
    }

    private boolean isSelected(Column column) {
      // An empty selection set means `*` (all columns)
      return selectedColumns().isEmpty() || selectedColumns().contains(column);
    }

    /**
     * Retrurn the list of columns whose values are used to distinguish one document from another.
     */
    private List<Column> docIdColumns(int keyDepth) {
      if (keyDepth < table().partitionKeyColumns().size()
          || keyDepth > table().primaryKeyColumns().size()) {
        throw new IllegalArgumentException("Invalid document identity depth: " + keyDepth);
      }

      List<Column> idColumns = table().primaryKeyColumns().subList(0, keyDepth);

      idColumns.forEach(
          column -> {
            if (!isSelected(column)) {
              throw new IllegalArgumentException(
                  "Required identity column is not selected: " + column);
            }
          });

      return idColumns;
    }

    /**
     * Returns a sub-list of the {@link #table() table's} clustering key columns that are explicitly
     * selected by the {@link #queries()}.
     */
    @Value.Lazy
    List<Column> docPathColumns() {
      return table().clusteringKeyColumns().stream()
          .filter(this::isSelected)
          .collect(Collectors.toList());
    }
  }
}
