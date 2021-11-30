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

package io.stargate.web.docsapi.service.write;

import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.core.SingleSource;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.web.docsapi.rx.RxUtils;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.JsonShreddedRow;
import io.stargate.web.docsapi.service.TimeSource;
import io.stargate.web.docsapi.service.write.db.DeleteQueryBuilder;
import io.stargate.web.docsapi.service.write.db.InsertQueryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class DocumentWriteService {

  private final TimeSource timeSource;
  private final DocsApiConfiguration config;
  private final InsertQueryBuilder insertQueryBuilder;
  private final DeleteQueryBuilder deleteQueryBuilder;
  private final Optional<Boolean> useLoggedBatches;

  @Inject
  public DocumentWriteService(TimeSource timeSource, DocsApiConfiguration config) {
    this.timeSource = timeSource;
    this.config = config;
    this.insertQueryBuilder = new InsertQueryBuilder(config.getMaxDepth());
    this.deleteQueryBuilder = new DeleteQueryBuilder();

    // only set log batches if explicitly disabled
    this.useLoggedBatches =
        Optional.ofNullable(System.getProperty("stargate.document_use_logged_batches"))
            .map(Boolean::parseBoolean)
            .filter(use -> !use);
  }

  /**
   * Writes a single document, without deleting the existing document with the same key. Please call
   * this method only if you are sure that the document with given ID does not exist.
   *
   * @param dataStore {@link DataStore}
   * @param keyspace Keyspace to store document in.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param rows Rows of this document.
   * @param context Execution content for profiling.
   * @return Single containing the {@link ResultSet} of the batch execution.
   */
  public Single<ResultSet> writeDocument(
      DataStore dataStore,
      String keyspace,
      String collection,
      String documentId,
      List<JsonShreddedRow> rows,
      ExecutionContext context) {
    // create and cache the insert query prepare
    return RxUtils.singleFromFuture(
            () -> {
              BuiltQuery<? extends BoundQuery> query =
                  insertQueryBuilder.buildQuery(dataStore::queryBuilder, keyspace, collection);
              return dataStore.prepare(query);
            })
        .cache()

        // then map to the list of the bound queries
        .map(
            insertPrepared -> {
              // take current timestamp for binding
              long timestamp = timeSource.currentTimeMicros();

              // create list of queries to send in batch
              List<BoundQuery> queries = new ArrayList<>(rows.size());

              // then all inserts
              rows.forEach(
                  row -> {
                    BoundQuery query =
                        insertQueryBuilder.bind(insertPrepared, documentId, row, timestamp);
                    queries.add(query);
                  });

              return queries;
            })

        // then execute batch
        .flatMap(
            boundQueries -> executeBatch(dataStore, boundQueries, context.nested("ASYNC INSERT")));
  }

  /**
   * Updates a single document, ensuring that existing document with the same key will be deleted
   * first.
   *
   * @param dataStore {@link DataStore}
   * @param keyspace Keyspace to store document in.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param rows Rows of this document.
   * @param context Execution content for profiling.
   * @return Single containing the {@link ResultSet} of the batch execution.
   */
  public Single<ResultSet> updateDocument(
      DataStore dataStore,
      String keyspace,
      String collection,
      String documentId,
      List<JsonShreddedRow> rows,
      ExecutionContext context) {
    // create and cache the remove query prepare
    Single<? extends Query<? extends BoundQuery>> deleteQueryPrepare =
        RxUtils.singleFromFuture(
                () -> {
                  BuiltQuery<? extends BoundQuery> query =
                      deleteQueryBuilder.buildQuery(dataStore::queryBuilder, keyspace, collection);
                  return dataStore.prepare(query);
                })
            .cache();

    // create and cache the insert query prepare
    Single<? extends Query<? extends BoundQuery>> insertQueryPrepare =
        RxUtils.singleFromFuture(
                () -> {
                  BuiltQuery<? extends BoundQuery> query =
                      insertQueryBuilder.buildQuery(dataStore::queryBuilder, keyspace, collection);
                  return dataStore.prepare(query);
                })
            .cache();

    // execute in parallel
    return Single.zip(
            deleteQueryPrepare,
            insertQueryPrepare,
            (deletePrepared, insertPrepared) -> {
              // take current timestamp for binding
              long timestamp = timeSource.currentTimeMicros();

              // create list of queries to send in batch
              List<BoundQuery> queries = new ArrayList<>(rows.size() + 1);

              // add delete
              BoundQuery deleteExistingQuery = deletePrepared.bind(timestamp - 1, documentId);
              queries.add(deleteExistingQuery);

              // then all inserts
              rows.forEach(
                  row -> {
                    BoundQuery query =
                        insertQueryBuilder.bind(insertPrepared, documentId, row, timestamp);
                    queries.add(query);
                  });

              return queries;
            })
        // then execute batch
        .flatMap(
            boundQueries -> executeBatch(dataStore, boundQueries, context.nested("ASYNC UPDATE")));
  }

  // executes batch
  private SingleSource<ResultSet> executeBatch(
      DataStore dataStore, List<BoundQuery> boundQueries, ExecutionContext context) {
    return RxUtils.singleFromFuture(
        () -> {
          // trace queries in context
          boundQueries.forEach(context::traceDeferredDml);

          // then execute batch
          boolean loggedBatch = useLoggedBatches.orElse(dataStore.supportsLoggedBatches());
          if (loggedBatch) {
            return dataStore.batch(boundQueries, ConsistencyLevel.LOCAL_QUORUM);
          } else {
            return dataStore.unloggedBatch(boundQueries, ConsistencyLevel.LOCAL_QUORUM);
          }
        });
  }
}
