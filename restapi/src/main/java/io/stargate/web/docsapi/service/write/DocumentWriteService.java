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
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.stargate.core.util.TimeSource;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.JsonShreddedRow;
import io.stargate.web.docsapi.service.write.db.DeleteQueryBuilder;
import io.stargate.web.docsapi.service.write.db.InsertQueryBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentWriteService {

  private static final Logger logger = LoggerFactory.getLogger(DocumentWriteService.class);

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
    this.deleteQueryBuilder = new DeleteQueryBuilder(config.getMaxDepth());

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
   * @param numericBooleans If numeric boolean should be stored.
   * @param context Execution content for profiling.
   * @return Single containing the {@link ResultSet} of the batch execution.
   */
  public Single<ResultSet> writeDocument(
      DataStore dataStore,
      String keyspace,
      String collection,
      String documentId,
      List<JsonShreddedRow> rows,
      boolean numericBooleans,
      ExecutionContext context) {
    // create and cache the insert query prepare
    return prepareInsertDocumentRowQuery(dataStore, keyspace, collection)

        // then map to the list of the bound queries
        .observeOn(Schedulers.computation())
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
                        insertQueryBuilder.bind(
                            insertPrepared, documentId, row, timestamp, numericBooleans);
                    queries.add(query);
                  });

              return queries;
            })

        // then execute batch
        .flatMap(
            boundQueries -> executeBatch(dataStore, boundQueries, context.nested("ASYNC INSERT")))

        // move away from the data store thread
        .observeOn(Schedulers.io());
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
   * @param numericBooleans If numeric boolean should be stored.
   * @param context Execution content for profiling.
   * @return Single containing the {@link ResultSet} of the batch execution.
   */
  public Single<ResultSet> updateDocument(
      DataStore dataStore,
      String keyspace,
      String collection,
      String documentId,
      List<JsonShreddedRow> rows,
      boolean numericBooleans,
      ExecutionContext context) {
    List<String> subDocumentPath = Collections.emptyList();
    return updateDocument(
        dataStore,
        keyspace,
        collection,
        documentId,
        subDocumentPath,
        rows,
        numericBooleans,
        context);
  }

  /**
   * Updates a single document, ensuring that existing document with the same key have the given
   * sub-path deleted.
   *
   * @param dataStore {@link DataStore}
   * @param keyspace Keyspace to store document in.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param subDocumentPath The sub-document path to delete.
   * @param rows Rows of this document.
   * @param numericBooleans If numeric boolean should be stored.
   * @param context Execution content for profiling.
   * @return Single containing the {@link ResultSet} of the batch execution.
   */
  public Single<ResultSet> updateDocument(
      DataStore dataStore,
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      List<JsonShreddedRow> rows,
      boolean numericBooleans,
      ExecutionContext context) {
    // check that update path matches the rows supplied
    // TODO this smells like a bad design
    //  for v2 we need to not include sub-path to shredding, and cover it here as a wrapper
    if (!subDocumentPath.isEmpty()) {
      int subDocumentPathSize = subDocumentPath.size();
      for (JsonShreddedRow row : rows) {
        List<String> path = row.getPath();
        if (path.size() < subDocumentPathSize
            || !Objects.equals(subDocumentPath, path.subList(0, subDocumentPathSize))) {
          ErrorCode code = ErrorCode.DOCS_API_UPDATE_PATH_NOT_MATCHING;
          throw new ErrorCodeRuntimeException(code);
        }
      }
    }

    // create and cache the remove query prepare
    Single<? extends Query<? extends BoundQuery>> deleteQueryPrepare =
        prepareDeleteDocumentQuery(dataStore, keyspace, collection, subDocumentPath);

    // create and cache the insert query prepare
    Single<? extends Query<? extends BoundQuery>> insertQueryPrepare =
        prepareInsertDocumentRowQuery(dataStore, keyspace, collection);

    // execute when both done
    return Single.zip(deleteQueryPrepare, insertQueryPrepare, Pair::of)
        .observeOn(Schedulers.computation())
        .map(
            pair -> {
              // take current timestamp for binding
              long timestamp = timeSource.currentTimeMicros();

              // create list of queries to send in batch
              List<BoundQuery> queries = new ArrayList<>(rows.size() + 1);

              // add delete
              BoundQuery deleteExistingQuery =
                  deleteQueryBuilder.bind(
                      pair.getLeft(), documentId, subDocumentPath, timestamp - 1);
              queries.add(deleteExistingQuery);

              // then all inserts
              rows.forEach(
                  row -> {
                    BoundQuery query =
                        insertQueryBuilder.bind(
                            pair.getRight(), documentId, row, timestamp, numericBooleans);
                    queries.add(query);
                  });

              return queries;
            })
        // then execute batch
        .flatMap(
            boundQueries -> executeBatch(dataStore, boundQueries, context.nested("ASYNC UPDATE")))

        // move away from the data store thread
        .observeOn(Schedulers.io());
  }

  /**
   * Deletes a single whole document.
   *
   * @param dataStore {@link DataStore}
   * @param keyspace Keyspace to delete a document from.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param rows Rows of this document.
   * @param numericBooleans If numeric boolean should be stored.
   * @param context Execution content for profiling.
   * @return Single containing the {@link ResultSet} of the batch execution.
   */
  public Single<ResultSet> deleteDocument(
      DataStore dataStore,
      String keyspace,
      String collection,
      String documentId,
      ExecutionContext context) {
    List<String> subDocumentPath = Collections.emptyList();
    return deleteDocument(dataStore, keyspace, collection, documentId, subDocumentPath, context);
  }

  /**
   * Deletes a single (sub-)document, ensuring that existing document with the same key have the
   * given sub-path deleted.
   *
   * @param dataStore {@link DataStore}
   * @param keyspace Keyspace to delete a document from.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param subDocumentPath The sub-document path to delete.
   * @param context Execution content for profiling.
   * @return Single containing the {@link ResultSet} of the batch execution.
   */
  public Single<ResultSet> deleteDocument(
      DataStore dataStore,
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      ExecutionContext context) {
    // create and cache the remove query prepare
    Single<? extends Query<? extends BoundQuery>> deleteQueryPrepare =
        prepareDeleteDocumentQuery(dataStore, keyspace, collection, subDocumentPath);

    // execute when prepared
    return deleteQueryPrepare
        .observeOn(Schedulers.computation())
        .map(
            prepared -> {
              // take current timestamp for binding
              long timestamp = timeSource.currentTimeMicros();

              // return bind delete
              return deleteQueryBuilder.bind(prepared, documentId, subDocumentPath, timestamp - 1);
            })
        // then execute single
        .flatMap(query -> executeSingle(dataStore, query, context.nested("ASYNC DELETE")))

        // move away from the data store thread
        .observeOn(Schedulers.io());
  }

  // create and cache the remove query prepare
  private Single<? extends Query<? extends BoundQuery>> prepareInsertDocumentRowQuery(
      DataStore dataStore, String keyspace, String collection) {

    return Single.fromCallable(
            () -> insertQueryBuilder.buildQuery(dataStore::queryBuilder, keyspace, collection))
        .flatMap(query -> Single.fromCompletionStage(dataStore.prepare(query)))
        .cache();
  }

  // create and cache delete query prepare
  private Single<? extends Query<? extends BoundQuery>> prepareDeleteDocumentQuery(
      DataStore dataStore, String keyspace, String collection, List<String> subDocumentPath) {

    return Single.fromCallable(
            () ->
                deleteQueryBuilder.buildQuery(
                    dataStore::queryBuilder, keyspace, collection, subDocumentPath))
        .flatMap(query -> Single.fromCompletionStage(dataStore.prepare(query)))
        .cache();
  }

  // executes batch
  private SingleSource<ResultSet> executeBatch(
      DataStore dataStore, List<BoundQuery> boundQueries, ExecutionContext context) {

    // trace queries in context
    boundQueries.forEach(context::traceDeferredDml);

    // then execute batch
    boolean loggedBatch = useLoggedBatches.orElse(dataStore.supportsLoggedBatches());
    CompletableFuture<ResultSet> future;
    if (loggedBatch) {
      future = dataStore.batch(boundQueries, ConsistencyLevel.LOCAL_QUORUM);
    } else {
      future = dataStore.unloggedBatch(boundQueries, ConsistencyLevel.LOCAL_QUORUM);
    }

    // return
    return Single.fromCompletionStage(future);
  }

  // executes single query
  private SingleSource<ResultSet> executeSingle(
      DataStore dataStore, BoundQuery boundQuery, ExecutionContext context) {

    // trace queries in context
    context.traceDeferredDml(boundQuery);

    // then execute batch
    CompletableFuture<ResultSet> future =
        dataStore.execute(boundQuery, ConsistencyLevel.LOCAL_QUORUM);

    // return
    return Single.fromCompletionStage(future);
  }
}
