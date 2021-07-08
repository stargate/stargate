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
import io.stargate.web.rx.RxUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/** Executes pre-built document queries, groups document rows and manages document pagination. */
public class QueryExecutor {

  private static final ByteBuffer EXHAUSTED_PAGE_STATE = ByteBuffer.allocate(0);
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

    List<Column> idColumns = idColumns(keyDepth, queries);
    Comparator<DocProperty> comparator = rowComparator(queries);

    List<ByteBuffer> pagingStates = CombinedPagingState.deserialize(queries.size(), pagingState);

    PagingStateTracker tracker = new PagingStateTracker(pagingStates);

    return execute(queries, comparator, pageSize, pagingStates, context)
        .map(p -> toSeed(p, comparator, idColumns))
        .concatWith(Single.just(TERM))
        .scan(tracker::combine)
        .filter(Accumulator::isComplete)
        .map(Accumulator::toDoc);
  }

  private Comparator<DocProperty> rowComparator(List<BoundQuery> queries) {
    List<Column> pathColumns = null;
    for (BoundQuery query : queries) {
      BuiltSelect select = (BuiltSelect) query.source().query();

      if (pathColumns == null) {
        pathColumns = select.table().clusteringKeyColumns();
      }
    }

    if (pathColumns == null) {
      throw new IllegalArgumentException("No query provided");
    }

    Comparator<DocProperty> comparator = Comparator.comparing(DocProperty::comparableKey);
    for (Column column : pathColumns) {
      comparator = comparator.thenComparing(p -> p.keyValue(column));
    }

    return comparator;
  }

  private List<Column> idColumns(int keyDepth, List<BoundQuery> queries) {
    List<Column> idColumns = null;
    for (BoundQuery query : queries) {
      BuiltSelect select = (BuiltSelect) query.source().query();
      if (keyDepth < 1 || keyDepth > select.table().primaryKeyColumns().size()) {
        throw new IllegalArgumentException("Invalid document identity depth: " + keyDepth);
      }

      if (idColumns == null) {
        idColumns = select.table().primaryKeyColumns().subList(0, keyDepth);
      }
    }

    if (idColumns == null) {
      throw new IllegalArgumentException("No query provided");
    }

    return idColumns;
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
    private DocProperty lastRow;

    private Accumulator() {
      id = null;
      rowComparator = null;
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
      this.lastRow = seedRow;
      this.rows = new ArrayList<>();
      this.pagingState = Collections.emptyList();
      this.next = null;
      this.complete = false;

      rows.add(seedRow);
    }

    private Accumulator(
        Accumulator complete, List<PagingStateSupplier> pagingState, Accumulator next) {
      this.id = complete.id;
      this.rowComparator = complete.rowComparator;
      this.docKey = complete.docKey;
      this.rows = complete.rows;
      this.pagingState = pagingState;
      this.lastRow = complete.lastRow;
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

      PagingStateBuilder pagingStateBuilder = new PagingStateBuilder(pagingState);

      return new RawDocument(id, docKey, pagingStateBuilder, docRows);
    }

    private Accumulator complete(PagingStateTracker tracker, Accumulator next) {
      rows.forEach(tracker::track);
      List<PagingStateSupplier> currentPagingState = tracker.slice();

      if (next == null) {
        if (currentPagingState.stream().noneMatch(PagingStateSupplier::hasPagingState)) {
          // Force a null paging state on the associated document
          currentPagingState = NoPagingState.INSTANCE;
        }
      }

      return new Accumulator(this, currentPagingState, next);
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
      if (rowComparator == null || lastRow == null) {
        throw new IllegalStateException("Cannot compare");
      }

      DocProperty otherRow = other.lastRow;

      if (rowComparator.compare(this.lastRow, otherRow) != 0) {
        rows.add(otherRow);
        this.lastRow = otherRow;
      }
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
      initialStates.stream().map(PreBuiltPagingState::new).forEach(states::add);
    }

    private Accumulator combine(Accumulator prev, Accumulator next) {
      return prev.combine(this, next);
    }

    public void track(DocProperty row) {
      states.set(row.queryIndex(), new RowPagingState(row));
    }

    public List<PagingStateSupplier> slice() {
      return new ArrayList<>(states);
    }
  }

  @Value.Immutable(lazyhash = true)
  public abstract static class DocProperty {

    abstract Page page();

    abstract Row row();

    abstract boolean lastInPage();

    abstract int queryIndex();

    @Value.Lazy
    ComparableKey<?> comparableKey() {
      return page().decorator().decoratePartitionKey(row());
    }

    String keyValue(Column column) {
      return row().getString(column.name());
    }

    @Value.Lazy
    @Override
    public String toString() {
      return row().toString();
    }
  }

  @Value.Immutable(lazyhash = true)
  public abstract static class Page {

    abstract ResultSet resultSet();

    @Value.Lazy
    RowDecorator decorator() {
      return resultSet().makeRowDecorator();
    }
  }

  private static class PreBuiltPagingState extends PagingStateSupplier {
    private final ByteBuffer pagingState;

    private PreBuiltPagingState(ByteBuffer pagingState) {
      this.pagingState = pagingState;
    }

    @Override
    public boolean hasPagingState() {
      // An empty paging state means the query was exhausted during previous execution.
      // A null paging state mean the query was not executed yet.
      return pagingState == null || pagingState.remaining() > 0;
    }

    @Override
    public ByteBuffer makePagingState(ResumeMode resumeMode) {
      return pagingState;
    }
  }

  private static class RowPagingState extends PagingStateSupplier {
    private final DocProperty row;

    private RowPagingState(DocProperty row) {
      this.row = row;
    }

    @Override
    public boolean hasPagingState() {
      return row.page().resultSet().getPagingState() != null;
    }

    @Override
    public ByteBuffer makePagingState(ResumeMode resumeMode) {
      if (row.lastInPage() && !hasPagingState()) {
        return EXHAUSTED_PAGE_STATE;
      }

      return row.page()
          .resultSet()
          .makePagingState(PagingPosition.ofCurrentRow(row.row()).resumeFrom(resumeMode).build());
    }
  }

  private static class NoPagingState extends PagingStateSupplier {
    private static final List<PagingStateSupplier> INSTANCE =
        Collections.singletonList(new NoPagingState());

    @Override
    public boolean hasPagingState() {
      return false;
    }

    @Override
    public ByteBuffer makePagingState(ResumeMode resumeMode) {
      return null;
    }
  }

  private static class PagingStateBuilder implements Function<ResumeMode, ByteBuffer> {
    private final List<PagingStateSupplier> pagingState;

    private PagingStateBuilder(List<PagingStateSupplier> pagingState) {
      this.pagingState = pagingState;
    }

    @Override
    public ByteBuffer apply(ResumeMode resumeMode) {
      List<ByteBuffer> pagingStateBuffers =
          pagingState.stream().map(s -> s.makePagingState(resumeMode)).collect(Collectors.toList());

      return CombinedPagingState.serialize(pagingStateBuffers);
    }
  }
}
