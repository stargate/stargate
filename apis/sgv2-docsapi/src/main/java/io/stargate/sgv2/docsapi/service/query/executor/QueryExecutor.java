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

package io.stargate.sgv2.docsapi.service.query.executor;

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
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.QueriesConfig;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Executes pre-built document queries, groups document rows and manages document pagination.
 *
 * <p>High-level explanations for some terms used in the query executor:
 *
 * <ul>
 *   <li><b>key-depth</b> - defines the amount of keys that should be considered in order to
 *       differentiate documents; minimum 1, when it only includes the document id; when larger than
 *       one, then it additionally includes the document paths at p0, p1, etc, thus being able to
 *       also query for sub-documents at different levels
 *   <li><b>multiple queries</b> - when multiple queries are given, they will be merged in an OR
 *       fashion, keeping the order of the documents as defined by the partition order; if two or
 *       more queries produce a same document, only one will be emitted (distinct).
 *   <li><b>combined page-states</b> - in order to support multiple queries, the page state is
 *       combined including the pointer to state from each query; this enables the executor to
 *       continue each query from it's own previous state; states are combined and uncombined by
 *       {@link CombinedPagingState}
 *   <li><b>exponential page-size</b> - if exponential page size is activated, the executor will
 *       exponentially increase the page size when executing the same query on the bridge while
 *       paging through; this can be used to gradually increase returned result set size when
 *       in-memory processing is needed; it's the best tread-off between a large amount of queries
 *       with small page-sizes and small amount of queries with large page-sizes
 *   <li><b>row-based page state</b> - executor enables optional fetching of the page state for each
 *       row; when activated the executor will use two available strategies based on the key depth;
 *       in case of depth=1, then it will use {@link
 *       io.stargate.bridge.proto.QueryOuterClass.ResumeMode#NEXT_PARTITION} as we are
 *       differentating between different documents; otherwise {@link
 *       io.stargate.bridge.proto.QueryOuterClass.ResumeMode#NEXT_ROW}
 * </ul>
 *
 * @author Dmitri Bourlatchkov
 * @author Ivan Senic
 */
@ApplicationScoped
public class QueryExecutor {

  public static final Accumulator TERM = new Accumulator();

  private final DocumentProperties documentProperties;

  private final QueriesConfig queriesConfig;

  private final StargateRequestInfo requestInfo;

  private final Comparator<DocumentProperty> comparator;

  @Inject
  public QueryExecutor(
      DocumentProperties documentProperties,
      QueriesConfig queriesConfig,
      StargateRequestInfo requestInfo) {
    this.documentProperties = documentProperties;
    this.queriesConfig = queriesConfig;
    this.requestInfo = requestInfo;

    // create reusable comparator
    // use all the doc path columns
    // note that this differs from V1,
    // as we can not know what clustering columns are selected in queries
    List<String> pathColumns = documentProperties.tableColumns().pathColumnNamesList();
    this.comparator = new DocumentPropertyComparator(pathColumns);
  }

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
    StargateBridge bridge = requestInfo.getStargateBridge();

    // deffer to wrap failures
    return Multi.createFrom()
        .deferred(
            () -> {

              // construct query data and id columns based on depth
              List<String> idColumns = docIdColumns(keyDepth);

              // get resume mode
              QueryOuterClass.ResumeMode resumeMode =
                  getResumeMode(fetchRowPaging, idColumns.size());

              // init the given paging states
              List<ByteBuffer> pagingStates = CombinedPagingState.deserialize(1, pagingState);
              PagingStateTracker tracker = new PagingStateTracker(pagingStates);

              // execute the single query
              Multi<DocumentProperty> documents =
                  executeQuery(
                      bridge, query, pageSize, exponentPageSize, pagingState, resumeMode, context);

              return accumulate(documents, idColumns, tracker);
            });
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
   * <p>Note: the queries must select all the partition key columns (in case of docs that's only the
   * key column) and zero or more clustering key columns with the total number of selected key
   * columns equal to the {@code keyDepth} parameter.
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

    StargateBridge bridge = requestInfo.getStargateBridge();

    // deffer to wrap failures
    return Multi.createFrom()
        .deferred(
            () -> {

              // construct query data and id columns based on depth
              List<String> idColumns = docIdColumns(keyDepth);

              // if row page should be fetched
              // then NEXT_ROW is used when we have more than one keys,
              // otherwise NEXT_PARTITION
              QueryOuterClass.ResumeMode resumeMode =
                  getResumeMode(fetchRowPaging, idColumns.size());

              // init the given paging states
              List<ByteBuffer> pagingStates =
                  CombinedPagingState.deserialize(queries.size(), pagingState);
              PagingStateTracker tracker = new PagingStateTracker(pagingStates);

              // execute all queries
              Multi<DocumentProperty> documents =
                  executeQueries(
                      bridge,
                      queries,
                      comparator,
                      pageSize,
                      exponentPageSize,
                      pagingStates,
                      resumeMode,
                      context);

              return accumulate(documents, idColumns, tracker);
            });
  }

  private QueryOuterClass.ResumeMode getResumeMode(boolean fetchRowPaging, int idColumnsSize) {
    // if row page should be fetched
    // then NEXT_ROW is used when we have more than one keys,
    // otherwise NEXT_PARTITION
    QueryOuterClass.ResumeMode resumeMode = null;
    if (fetchRowPaging) {
      resumeMode =
          idColumnsSize > 1
              ? QueryOuterClass.ResumeMode.NEXT_ROW
              : QueryOuterClass.ResumeMode.NEXT_PARTITION;
    }
    return resumeMode;
  }

  // accumulates the document properties into RawDocument instances
  private Multi<RawDocument> accumulate(
      Multi<DocumentProperty> documents, List<String> idColumns, PagingStateTracker tracker) {
    Multi<Accumulator> accumulatorMulti = documents.map(p -> toSeed(p, comparator, idColumns));

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
  }

  // executes a single queries and provides Multi of DocumentProperty
  // we can short circuit and avoid any merges and transformations
  private Multi<DocumentProperty> executeQuery(
      StargateBridge stargateBridge,
      QueryOuterClass.Query query,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState,
      QueryOuterClass.ResumeMode resumeMode,
      ExecutionContext context) {

    ByteBuffer queryPagingState = pagingState;
    if (queryPagingState != null) {
      queryPagingState = queryPagingState.slice();
    }

    // execute that query
    // comparable bytes with singe query not needed
    return queryBridge(
            stargateBridge, query, pageSize, exponentPageSize, queryPagingState, resumeMode, false)

        // for each result set, transform to doc property
        .onItem()
        .transformToMultiAndConcatenate(
            rs -> Multi.createFrom().iterable(properties(0, query, rs, context)));
  }

  // executes multiple queries and provides Multi of DocumentProperty
  // this means that each item represents one value of a single document
  // these are ordered, first by partition, then by the value of the included sub-paths
  private Multi<DocumentProperty> executeQueries(
      StargateBridge stargateBridge,
      List<QueryOuterClass.Query> queries,
      Comparator<DocumentProperty> comparator,
      int pageSize,
      boolean exponentPageSize,
      List<ByteBuffer> pagingStates,
      QueryOuterClass.ResumeMode resumeMode,
      ExecutionContext context) {

    // for each query
    List<Flowable<DocumentProperty>> allDocuments =
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
                  // comparable bytes with multiple queries needed
                  return queryBridge(
                          stargateBridge,
                          query,
                          pageSize,
                          exponentPageSize,
                          queryPagingState,
                          resumeMode,
                          true)

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
    Flowable<DocumentProperty> orderedPublisher =
        Flowables.orderedMerge(allDocuments, comparator, false, 1);

    // transform back to the multi
    return Multi.createFrom().publisher(orderedPublisher);
  }

  // executes a single query and returns Multi of the ResultSet
  // each result set represents a result of a single trip to the data store
  private Multi<QueryOuterClass.ResultSet> queryBridge(
      StargateBridge stargateBridge,
      QueryOuterClass.Query query,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState,
      QueryOuterClass.ResumeMode resumeMode,
      boolean comparableBytesNeeded) {
    // An empty paging state means the query was exhausted during previous execution
    if (pagingState != null && pagingState.remaining() == 0) {
      return Multi.createFrom().empty();
    }

    // construct initial state for the query
    BytesValue pagingStateValue =
        pagingState != null
            ? BytesValue.newBuilder().setValue(ByteString.copyFrom(pagingState)).build()
            : null;
    QueryState initialState = ImmutableQueryState.of(pageSize, pagingStateValue);

    return Multi.createBy()
        .repeating()
        .uni(
            () -> new AtomicReference<>(initialState),
            stateRef -> {
              QueryState state = stateRef.get();

              // create params, ensure:
              // 1. read consistency
              // 2. needed page size
              // 3. enriched if needed
              // 4. resume mode if defined
              QueryOuterClass.Consistency consistency = queriesConfig.consistency().reads();
              QueryOuterClass.ConsistencyValue.Builder consistencyValue =
                  QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency);
              boolean enriched = comparableBytesNeeded || null != resumeMode;
              QueryOuterClass.QueryParameters.Builder params =
                  QueryOuterClass.QueryParameters.newBuilder()
                      .setConsistency(consistencyValue)
                      .setPageSize(Int32Value.of(state.pageSize()))
                      .setEnriched(enriched);

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
                        QueryState nextState =
                            state.next(
                                resultSet.getPagingState(),
                                exponentPageSize,
                                documentProperties.maxSearchPageSize());
                        stateRef.set(nextState);

                        return resultSet;
                      });
            })

        // and do fetch results until we have a paging state
        // if necessary of course, handled by the down stream
        .whilst(QueryOuterClass.ResultSet::hasPagingState);
  }

  /**
   * Converts a single page of results into {@link DocumentProperty} objects to maintain an
   * association of rows to their respective {@link
   * io.stargate.bridge.proto.QueryOuterClass.ResultSet} objects and queries (the latter is needed
   * for tracking the combined paging state).
   */
  private Iterable<DocumentProperty> properties(
      int queryIndex,
      QueryOuterClass.Query query,
      QueryOuterClass.ResultSet rs,
      ExecutionContext context) {
    // get rows & trace results
    List<QueryOuterClass.Row> rows = rs.getRowsList();
    context.traceCqlResult(query.getCql(), rows.size());

    // then convert each row to row wrapper and construct doc prop
    List<QueryOuterClass.ColumnSpec> columnsList = rs.getColumnsList();
    Function<QueryOuterClass.Row, RowWrapper> wrapperFunction = RowWrapper.forColumns(columnsList);
    List<DocumentProperty> properties = new ArrayList<>(rows.size());
    int count = rows.size();
    for (QueryOuterClass.Row row : rows) {
      boolean last = --count <= 0;
      RowWrapper rowWrapper = wrapperFunction.apply(row);
      properties.add(
          ImmutableDocumentProperty.builder()
              .queryIndex(queryIndex)
              .page(rs)
              .rowWrapper(rowWrapper)
              .lastInPage(last)
              .build());
    }
    return properties;
  }

  // creates a new Accumulator for a single DocumentProperty
  private Accumulator toSeed(
      DocumentProperty property, Comparator<DocumentProperty> comparator, List<String> keyColumns) {
    DocumentTableProperties tableProperties = documentProperties.tableProperties();
    RowWrapper row = property.rowWrapper();

    String id = row.getString(tableProperties.keyColumnName());
    ImmutableList.Builder<String> docKey = ImmutableList.builder();
    for (String columns : keyColumns) {
      docKey.add(Objects.requireNonNull(row.getString(columns)));
    }

    return new Accumulator(id, comparator, docKey.build(), property);
  }

  // resolves what columns will be considered as keys for the document aggregation
  // always includes the document id,
  // plus set of path columns based on the key depth
  // for example, having keyDepth=3 would return [key, p0, p1]
  private List<String> docIdColumns(int keyDepth) {
    if (keyDepth < 1 || keyDepth > documentProperties.maxDepth() + 1) {
      throw new IllegalArgumentException("Invalid document identity depth: " + keyDepth);
    }

    DocumentTableProperties tableProperties = documentProperties.tableProperties();

    // returns the key column always
    // plus keyDepth - 1 path columns
    List<String> result = new ArrayList<>(keyDepth);
    result.add(tableProperties.keyColumnName());
    for (int i = 0; i < keyDepth - 1; i++) {
      result.add(tableProperties.pathColumnName(i));
    }
    return result;
  }

  // knows how to accumulate a set of document properties
  // into a RawDocument
  private static class Accumulator {
    private final String id;
    private final Comparator<DocumentProperty> rowComparator;
    private final List<String> docKey;
    private final List<DocumentProperty> rows;
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
        Comparator<DocumentProperty> rowComparator,
        List<String> docKey,
        DocumentProperty seedRow) {
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
        List<DocumentProperty> rows,
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
          this.rows.stream().map(DocumentProperty::rowWrapper).collect(Collectors.toList());

      CombinedPagingState combinedPagingState = new CombinedPagingState(pagingState);

      return ImmutableRawDocument.of(id, docKey, combinedPagingState, docRows);
    }

    private Accumulator complete(PagingStateTracker tracker, Accumulator next) {
      // Deduplicate included rows and run them through the paging state tracker.
      // Note: the `rows` should already be in the order consistent with `rowComparator`.
      List<DocumentProperty> finalRows = new ArrayList<>(rows.size());
      DocumentProperty last = null;
      for (DocumentProperty row : rows) {
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

  // knows how to track paging states from a collection of queries
  // by saving the PagingStateSupplier in the same index as the query index
  private static class PagingStateTracker {
    private final ArrayList<PagingStateSupplier> states;

    private PagingStateTracker(List<ByteBuffer> initialStates) {
      states = new ArrayList<>(initialStates.size());
      initialStates.stream().map(PagingStateSupplier::fixed).forEach(states::add);
    }

    private Accumulator combine(Accumulator prev, Accumulator next) {
      return prev.combine(this, next);
    }

    public void track(DocumentProperty row) {
      states.set(row.queryIndex(), row);
    }

    public List<PagingStateSupplier> slice() {
      return new ArrayList<>(states);
    }
  }
}
