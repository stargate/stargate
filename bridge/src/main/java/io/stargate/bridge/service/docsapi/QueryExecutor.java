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
package io.stargate.bridge.service.docsapi;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import hu.akarnokd.rxjava3.operators.ExpandStrategy;
import hu.akarnokd.rxjava3.operators.FlowableTransformers;
import hu.akarnokd.rxjava3.operators.Flowables;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.stargate.bridge.service.BridgeService;
import io.stargate.bridge.service.docsapi.ImmutableQueryData;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.proto.QueryOuterClass;
//import io.stargate.db.ComparableKey;
//import io.stargate.db.ImmutableParameters;
//import io.stargate.db.PagingPosition;
//import io.stargate.db.PagingPosition.ResumeMode;
//import io.stargate.db.RowDecorator;
//import io.stargate.db.datastore.DataStore;
//import io.stargate.db.datastore.ResultSet;
//import io.stargate.db.datastore.Row;
//import io.stargate.db.query.BoundQuery;
//import io.stargate.db.query.builder.BuiltSelect;
//import io.stargate.db.schema.AbstractTable;
//import io.stargate.db.schema.Column;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Executes pre-built document queries, groups document rows and manages document pagination. */
public class QueryExecutor {
  private final BridgeService bridgeService;
  private final DocsApiConfiguration config;

  public QueryExecutor(BridgeService bridgeService, DocsApiConfiguration configuration) {
    this.bridgeService = bridgeService;
    this.config = configuration;
  }

  /**
   * Runs the provided query, then groups the rows into {@link RawDocument} objects with key depth
   * of 1.
   *
   * @see #queryDocs(int, List, int, boolean, ByteBuffer, ExecutionContext)
   */
  public Flowable<RawDocument> queryDocs(
      String cql,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState
      /* ExecutionContext context */) {
    return queryDocs(1, cql, pageSize, exponentPageSize, pagingState);
  }

  /**
   * Runs the provided query, then groups the rows into {@link RawDocument} objects according to
   * {@code keyDepth} primary key values.
   *
   * @see #queryDocs(int, List, int, boolean, ByteBuffer, ExecutionContext)
   */
  public Flowable<RawDocument> queryDocs(
      int keyDepth,
      String cql,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState
      /* ExecutionContext context */) {
    return queryDocs(keyDepth, ImmutableList.of(cql), pageSize, exponentPageSize, pagingState);
  }

  /**
   * Runs the provided queries in parallel and merges their result sets according to the selected
   * primary key columns, then groups the merged rows into {@link RawDocument} objects with key
   * depth of 1.
   *
   * @see #queryDocs(int, List, int, boolean, ByteBuffer, ExecutionContext)
   */
  public Flowable<RawDocument> queryDocs(
      List<String> cqlQueries,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState
      /* ExecutionContext context */) {
    return queryDocs(1, cqlQueries, pageSize, exponentPageSize, pagingState);
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
   * @param cqlQueries the queries to run (one or more).
   * @param pageSize the storage-level page size to use (1 or greater).
   * @param exponentPageSize if the storage-level page size should be exponentially increase with
   *     every next hop to the data store
   * @param pagingState the storage-level page state to use (may be {@code null}).
   * @param context the query execution context for profiling.
   * @return a flow of documents grouped by the primary key values up to {@code keyDepth} and
   *     ordered according to the natural Cassandra result set order (ring order on the partition
   *     key and default order on clustering keys).
   */
  public Flowable<RawDocument> queryDocs(
      int keyDepth,
      List<String> cqlQueries,
      int pageSize,
      boolean exponentPageSize,
      ByteBuffer pagingState
      /* ExecutionContext context */) {
    if (pageSize <= 0) {
      // Note: if page size is not set, C* will ignore paging state, but subsequent pages may be
      // requested by the execute() method, so we require a specific page size for all queries here.
      throw new IllegalArgumentException("Unsupported page size: " + pageSize);
    }

    QueryData queryData = ImmutableQueryData.builder().queries(cqlQueries).build();

    List<Column> idColumns = queryData.docIdColumns(keyDepth);
    Comparator<DocProperty> comparator = rowComparator(queryData);

    List<ByteBuffer> pagingStates = CombinedPagingState.deserialize(cqlQueries.size(), pagingState);

    PagingStateTracker tracker = new PagingStateTracker(pagingStates);

    return execute(cqlQueries, comparator, pageSize, exponentPageSize, pagingStates)
        .map(p -> toSeed(p, comparator, idColumns))
        .concatWith(Single.just(Accumulator.TERM))
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
      List<String> cqlQueries,
      Comparator<DocProperty> comparator,
      int pageSize,
      boolean exponentPageSize,
      List<ByteBuffer> pagingState
      /* ExecutionContext context */) {
    List<Flowable<DocProperty>> flows = new ArrayList<>(cqlQueries.size());
    int idx = 0;
    for (String query : cqlQueries) {
      final int finalIdx = idx++;
      ByteBuffer queryPagingState = pagingState.get(finalIdx);
      if (queryPagingState != null) {
        queryPagingState = queryPagingState.slice();
      }

      flows.add(
          execute(query, pageSize, exponentPageSize, queryPagingState)
              .flatMap(
                  rs -> Flowable.fromIterable(properties(finalIdx, query, rs)),
                  1)); // max concurrency 1
    }

    return Flowables.orderedMerge(flows, comparator, false, 1); // prefetch 1
  }

  public Flowable<QueryOuterClass.ResultSet> execute(
      String query, int pageSize, boolean exponentPageSize, ByteBuffer pagingState) {
    // An empty paging state means the query was exhausted during previous execution
    if (pagingState != null && pagingState.remaining() == 0) {
      return Flowable.empty();
    }

    AtomicInteger effectivePageSize = new AtomicInteger(pageSize);
    return fetchPage(query, pageSize, pagingState)
        .compose( // Expand BREADTH_FIRST to reduce the number of "proactive" page requests
            FlowableTransformers.expand(
                rs -> {
                  // in case we have the exponent page size, increase the current value by itself
                  // but ensure we are never over maximum
                  int nextPageSize =
                      exponentPageSize
                          ? effectivePageSize.updateAndGet(
                              x -> Math.min(x * 2, config.getMaxStoragePageSize()))
                          : pageSize;
                  return fetchNext(rs, nextPageSize, query);
                },
                ExpandStrategy.BREADTH_FIRST,
                1));
  }

  private Flowable<QueryOuterClass.Response> fetchPage(String query, int pageSize, ByteBuffer pagingState) {
    Supplier<CompletableFuture<QueryOuterClass.Response>> supplier =
        () -> {
          CompletableFuture<QueryOuterClass.Response> future = new CompletableFuture<>();
          bridgeService.executeQuery(
                  QueryOuterClass.Query.newBuilder()
                          .setCql(query)
                          .setParameters(
                                  QueryOuterClass.QueryParameters.newBuilder()
                                          .setPageSize(Int32Value.newBuilder().setValue(pageSize).build())
                                          .setPagingState(BytesValue.newBuilder().setValue(ByteString.copyFrom(pagingState.array())).build())
                                          .build()
                          ).build(),
                  new QueryExecutorResponseObserver(future)
          );
          return future;
        };

    return RxUtils.singleFromFuture(supplier)
        .observeOn(Schedulers.io())
        .toFlowable()
        .compose(FlowableConnectOnRequest.with()) // separate subscription from query execution
        .take(1);
  }

  private Flowable<QueryOuterClass.Response> fetchNext(QueryOuterClass.ResultSet rs, int pageSize, String query) {
    BytesValue nextPagingState = rs.getPagingState();
    if (nextPagingState == null) {
      return Flowable.empty();
    } else {
      return fetchPage(query, pageSize, ByteBuffer.wrap(nextPagingState.getValue().toByteArray()));
    }
  }

  private Accumulator toSeed(
      DocProperty property, Comparator<DocProperty> comparator, List<Column> keyColumns) {
    Row row = property.row();

    String id = row.getString(DocsApiConstants.KEY_COLUMN_NAME);
    ImmutableList.Builder<String> docKey = ImmutableList.builder();
    for (Column c : keyColumns) {
      docKey.add(Objects.requireNonNull(row.getString(c.name())));
    }

    return new Accumulator(id, comparator, docKey.build(), property);
  }

  /**
   * Converts a single page of results into {@link DocProperty} objects to maintain an association
   * of rows to their respective {@link QueryOuterClass.ResultSet} objects and queries (the latter is needed for
   * tracking the combined paging state).
   */
  private Iterable<DocProperty> properties(
      int queryIndex, String query, QueryOuterClass.ResultSet rs) {
    List<QueryOuterClass.Row> rows = rs.getRowsList();
    // context.traceCqlResult(query, rows.size());

    Page page = ImmutablePage.builder().resultSet(rs).build();
    List<DocProperty> properties = new ArrayList<>(rows.size());
    int count = rows.size();
    for (QueryOuterClass.Row row : rows) {
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
}
