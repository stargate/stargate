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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.service.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import hu.akarnokd.rxjava3.operators.Flowables;
import io.reactivex.rxjava3.core.Flowable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.converters.multi.MultiRx3Converters;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.ImmutableRowWrapper;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.model.ImmutableRawDocument;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.model.paging.CombinedPagingState;
import io.stargate.sgv2.docsapi.service.query.model.paging.PagingStateSupplier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes pre-built document queries, groups document rows and manages document pagination. */
@ApplicationScoped
public class QueryExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

  public final Accumulator TERM = new Accumulator();

  @Inject DocumentProperties documentProperties;

  @Inject StargateRequestInfo requestInfo;

  /**
   * Runs the provided query, then groups the rows into {@link RawDocument} objects with key depth
   * of 1.
   *
   * @see #queryDocs(int, List, int, boolean, ByteBuffer, boolean, ExecutionContext)
   */
  public Multi<RawDocument> queryDocs(
      QueryOuterClass.Query query,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState,
      boolean fetchRowPaging,
      ExecutionContext context) {
    return queryDocs(1, query, pageSize, exponentPageSize, pagingState, fetchRowPaging, context);
  }

  /**
   * Runs the provided query, then groups the rows into {@link RawDocument} objects according to
   * {@code keyDepth} primary key values.
   *
   * @see #queryDocs(int, List, int, boolean, ByteBuffer, boolean, ExecutionContext)
   */
  public Multi<RawDocument> queryDocs(
      int keyDepth,
      QueryOuterClass.Query query,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState,
      boolean fetchRowPaging,
      ExecutionContext context) {
    return queryDocs(
        keyDepth,
        ImmutableList.of(query),
        pageSize,
        exponentPageSize,
        pagingState,
        fetchRowPaging,
        context);
  }

  /**
   * Runs the provided queries in parallel and merges their result sets according to the selected
   * primary key columns, then groups the merged rows into {@link RawDocument} objects with key
   * depth of 1.
   *
   * @see #queryDocs(int, List, int, boolean, ByteBuffer, boolean, ExecutionContext)
   */
  public Multi<RawDocument> queryDocs(
      List<QueryOuterClass.Query> queries,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState,
      boolean fetchRowPaging,
      ExecutionContext context) {
    return queryDocs(1, queries, pageSize, exponentPageSize, pagingState, fetchRowPaging, context);
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
   * @param exponentPageSize if the storage-level page size should be exponentially increase with
   *     every next hop to the data store
   * @param pagingState the storage-level page state to use (may be {@code null}).
   * @param fetchRowPaging if the paging state for each row should be fetched, this should be <code>
   *     true</code> when queries will be reference ones for generating a user-facing document
   *     paging state
   * @param context the query execution context for profiling.
   * @return a flow of documents grouped by the primary key values up to {@code keyDepth} and
   *     ordered according to the natural Cassandra result set order (ring order on the partition
   *     key and default order on clustering keys).
   */
  public Multi<RawDocument> queryDocs(
      int keyDepth,
      List<QueryOuterClass.Query> queries,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState,
      boolean fetchRowPaging,
      ExecutionContext context) {

    // deffer to wrap failures
    return Multi.createFrom()
        .deferred(
            () -> {

              // construct query data and id columns based on depth
              QueryData queryData = ImmutableQueryData.of(queries, documentProperties);
              List<String> idColumns = queryData.docIdColumns(keyDepth);

              // if row page should be fetched
              // then NEXT_ROW is used when we have more than one keys,
              // otherwise NEXT_PARTITION
              QueryOuterClass.ResumeMode resumeMode = null;
              if (fetchRowPaging) {
                resumeMode =
                    idColumns.size() > 1
                        ? QueryOuterClass.ResumeMode.NEXT_ROW
                        : QueryOuterClass.ResumeMode.NEXT_PARTITION;
              }

              // create comparator
              Comparator<DocProperty> comparator = rowComparator(queryData);

              // init the given paging states
              List<ByteBuffer> pagingStates =
                  CombinedPagingState.deserialize(queries.size(), pagingState);
              PagingStateTracker tracker = new PagingStateTracker(pagingStates);

              // execute all queries
              StargateBridge bridge = requestInfo.getStargateBridge();
              Multi<Accumulator> accumulatorMulti =
                  executeQueries(
                          bridge,
                          queries,
                          comparator,
                          pageSize,
                          exponentPageSize,
                          pagingStates,
                          resumeMode,
                          context)

                      // map to seed
                      .map(p -> toSeed(p, comparator, idColumns));

              // then add the TERM to the end
              return Multi.createBy()
                  .concatenating()
                  .streams(accumulatorMulti, Multi.createFrom().items(TERM))

                  // can with tracker combine
                  .onItem()
                  .scan(tracker::combine)

                  // filter only complete elements
                  .filter(Accumulator::isComplete)

                  // and map to the document
                  .map(Accumulator::toDoc);
            });
  }

  private Multi<DocProperty> executeQueries(
      StargateBridge stargateBridge,
      List<QueryOuterClass.Query> queries,
      Comparator<DocProperty> comparator,
      int pageSize,
      boolean exponentPageSize,
      List<ByteBuffer> pagingStates,
      QueryOuterClass.ResumeMode resumeMode,
      ExecutionContext context) {

    // for each query
    List<Flowable<DocProperty>> allDocuments =
        Streams.mapWithIndex(
                queries.stream(),
                (query, index) -> {

                  // get the initial paging state for the query
                  int intIndex = (int) index;
                  ByteBuffer queryPagingState = pagingStates.get(intIndex);
                  if (queryPagingState != null) {
                    queryPagingState = queryPagingState.slice();
                  }

                  // execute that query
                  return executeQuery(
                          stargateBridge,
                          query,
                          pageSize,
                          exponentPageSize,
                          queryPagingState,
                          resumeMode)

                      // for each result set, transform to doc property
                      .onItem()
                      .transformToMultiAndConcatenate(
                          rs ->
                              Multi.createFrom().iterable(properties(intIndex, query, rs, context)))

                      // convert to rx-java3, as mutiny does not support ordered merge
                      // see https://github.com/smallrye/smallrye-mutiny/discussions/938
                      .convert()
                      .with(MultiRx3Converters.toFlowable());
                })
            .toList();

    // do order merge with prefetch 1
    Flowable<DocProperty> orderedPublisher =
        Flowables.orderedMerge(allDocuments, comparator, false, 1);

    // transform back to the multi
    return Multi.createFrom().publisher(orderedPublisher);
  }

  private Multi<QueryOuterClass.ResultSet> executeQuery(
      StargateBridge stargateBridge,
      QueryOuterClass.Query query,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState,
      QueryOuterClass.ResumeMode resumeMode) {
    // An empty paging state means the query was exhausted during previous execution
    if (pagingState != null && pagingState.remaining() == 0) {
      return Multi.createFrom().empty();
    }

    // construct initial state for the query
    BytesValue pagingStateValue =
        pagingState != null
            ? BytesValue.newBuilder().setValue(ByteString.copyFrom(pagingState)).build()
            : null;
    QueryState initialState =
        ImmutableQueryState.of(
            pageSize, documentProperties.maxSearchPageSize(), exponentPageSize, pagingStateValue);

    return Multi.createBy()
        .repeating()
        .uni(
            () -> new AtomicReference<>(initialState),
            stateRef -> {
              QueryState state = stateRef.get();

              // create params
              QueryOuterClass.QueryParameters.Builder params =
                  QueryOuterClass.QueryParameters.newBuilder()
                      .setPageSize(Int32Value.of(state.pageSize()))
                      .setEnriched(true);

              // set resume mode if not null
              if (null != resumeMode) {
                params.setResumeMode(
                    QueryOuterClass.ResumeModeValue.newBuilder().setValue(resumeMode).build());
              }

              // if we have paging state, set
              if (null != state.pagingState()) {
                params.setPagingState(state.pagingState());
              }

              // final query is same as the original, just with different params
              QueryOuterClass.Query finalQuery =
                  QueryOuterClass.Query.newBuilder(query).setParameters(params).buildPartial();

              // execute
              return stargateBridge
                  .executeQuery(finalQuery)
                  .map(
                      response -> {
                        // update next state
                        QueryOuterClass.ResultSet resultSet = response.getResultSet();
                        QueryState nextState = state.next(resultSet.getPagingState());
                        stateRef.set(nextState);

                        return resultSet;
                      });
            })

        // and do fetch results until we have a paging state
        // if necessary of course, handled by the down stream
        .whilst(QueryOuterClass.ResultSet::hasPagingState);
  }

  /**
   * Converts a single page of results into {@link DocProperty} objects to maintain an association
   * of rows to their respective {@link io.stargate.bridge.proto.QueryOuterClass.ResultSet} objects
   * and queries (the latter is needed for tracking the combined paging state).
   */
  private Iterable<DocProperty> properties(
      int queryIndex,
      QueryOuterClass.Query query,
      QueryOuterClass.ResultSet rs,
      ExecutionContext context) {
    // get rows & trace results
    List<QueryOuterClass.Row> rows = rs.getRowsList();
    context.traceCqlResult(query.getCql(), rows.size());

    // construct page
    Page page = ImmutablePage.builder().resultSet(rs).build();

    // then convert each row to row wrapper and construct doc prop
    List<QueryOuterClass.ColumnSpec> columnsList = rs.getColumnsList();
    List<DocProperty> properties = new ArrayList<>(rows.size());
    int count = rows.size();
    for (QueryOuterClass.Row row : rows) {
      boolean last = --count <= 0;
      RowWrapper rowWrapper = ImmutableRowWrapper.of(columnsList, row);
      properties.add(
          ImmutableDocProperty.builder()
              .queryIndex(queryIndex)
              .page(page)
              .rowWrapper(rowWrapper)
              .lastInPage(last)
              .build());
    }
    return properties;
  }

  private Accumulator toSeed(
      DocProperty property, Comparator<DocProperty> comparator, List<String> keyColumns) {
    DocumentTableProperties tableProperties = documentProperties.tableProperties();
    RowWrapper row = property.rowWrapper();

    String id = row.getString(tableProperties.keyColumnName());
    ImmutableList.Builder<String> docKey = ImmutableList.builder();
    for (String columns : keyColumns) {
      docKey.add(Objects.requireNonNull(row.getString(columns)));
    }

    return new Accumulator(id, comparator, docKey.build(), property);
  }

  /**
   * Builds a comparator that follows the natural order of rows in document tables, but limited to
   * the selected clustering columns (i.e., "path" columns).
   */
  private Comparator<DocProperty> rowComparator(QueryData queries) {
    List<Comparator<DocProperty>> comparators = new ArrayList<>();

    // always first compare by the comparable bytes
    comparators.add(DocProperty.COMPARABLE_BYTES_COMPARATOR);

    for (String column : queries.docPathColumns()) {
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

  @Value.Immutable
  public interface QueryState {

    @Value.Parameter
    int pageSize();

    @Value.Parameter
    int maxPageSize();

    @Value.Parameter
    boolean exponentPageSize();

    @Value.Parameter
    @Nullable
    BytesValue pagingState();

    default QueryState next(BytesValue nextPagingState) {
      int nextPageSize = exponentPageSize() ? Math.min(pageSize() * 2, maxPageSize()) : pageSize();

      return ImmutableQueryState.of(
          nextPageSize, maxPageSize(), exponentPageSize(), nextPagingState);
    }
  }

  /** A thin wrapper around {@link QueryOuterClass.ResultSet}. */
  @Value.Immutable(lazyhash = true)
  public abstract static class Page {

    abstract QueryOuterClass.ResultSet resultSet();
  }

  /**
   * Represents individual rows that provide values for document properties.
   *
   * <p>This class links a {@link RowWrapper} to its source {@link Page} and the {@link
   * #queryIndex() query} that produced it.
   */
  @Value.Immutable(lazyhash = true)
  public abstract static class DocProperty implements PagingStateSupplier {

    public static final Comparator<DocProperty> COMPARABLE_BYTES_COMPARATOR =
        (docProperty1, docProperty2) ->
            ByteString.unsignedLexicographicalComparator()
                .compare(docProperty1.comparableKey(), docProperty2.comparableKey());

    /** Page where the row that describes this document property originates from. */
    abstract Page page();

    /** A single row wrapper in {@link RowWrapper}. */
    abstract RowWrapper rowWrapper();

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
    ByteString comparableKey() {
      QueryOuterClass.Row row = rowWrapper().row();
      return row.getComparableBytes().getValue();
    }

    String keyValue(String column) {
      if (rowWrapper().columnExists(column)) {
        return rowWrapper().getString(column);
      }
      return null;
    }

    @Override
    public ByteBuffer makePagingState() {
      // Note: is current row was not the last one in its page the higher-level request may
      // choose to resume fetching from the next row in current page even if the page itself does
      // not have a paging state. Therefore, in this case we cannot return EXHAUSTED_PAGE_STATE.
      if (lastInPage() && !page().resultSet().hasPagingState()) {
        return CombinedPagingState.EXHAUSTED_PAGE_STATE;
      }

      // ensure we have a row page state
      QueryOuterClass.Row row = rowWrapper().row();
      if (!row.hasPagingState()) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_ROW_PAGE_STATE_MISSING);
      }

      BytesValue pagingState = row.getPagingState();
      return pagingState.getValue().asReadOnlyByteBuffer();
    }

    @Value.Lazy
    @Override
    public String toString() {
      return rowWrapper().toString();
    }
  }

  /** A helper class for dealing with a list of related queries. */
  @Value.Immutable(lazyhash = true)
  public abstract static class QueryData {

    @Value.Parameter
    abstract List<QueryOuterClass.Query> queries();

    @Value.Parameter
    abstract DocumentProperties documentProperties();

    /**
     * Returns the list of columns whose values are used to distinguish one document from another.
     * It's based purely in key-depth.
     */
    List<String> docIdColumns(int keyDepth) {
      if (keyDepth < 1 || keyDepth > documentProperties().maxDepth() + 1) {
        throw new IllegalArgumentException("Invalid document identity depth: " + keyDepth);
      }

      DocumentTableProperties tableProperties = documentProperties().tableProperties();

      // returns the key column always
      // plus keyDepth - 1 path columns
      List<String> result = new ArrayList<>(keyDepth);
      result.add(tableProperties.keyColumnName());
      for (int i = 0; i < keyDepth - 1; i++) {
        result.add(tableProperties.pathColumnName(i));
      }
      return result;
    }

    @Value.Lazy
    public List<String> docPathColumns() {
      // TODO we push all columns as we can not know what's selected
      //  make cached or smth
      return documentProperties().tableColumns().pathColumnNames().stream().sorted().toList();
    }
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

      List<RowWrapper> docRows =
          this.rows.stream().map(DocProperty::rowWrapper).collect(Collectors.toList());

      CombinedPagingState combinedPagingState = new CombinedPagingState(pagingState);

      return ImmutableRawDocument.of(id, docKey, combinedPagingState, docRows);
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
}
