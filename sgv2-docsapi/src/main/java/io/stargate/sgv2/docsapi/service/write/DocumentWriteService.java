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

package io.stargate.sgv2.docsapi.service.write;

import io.opentelemetry.extension.annotations.WithSpan;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.config.QueriesConfig;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import io.stargate.sgv2.docsapi.service.util.DocsApiUtils;
import io.stargate.sgv2.docsapi.service.util.TimeSource;
import io.stargate.sgv2.docsapi.service.write.db.AbstractDeleteQueryBuilder;
import io.stargate.sgv2.docsapi.service.write.db.DeleteDocumentQueryBuilder;
import io.stargate.sgv2.docsapi.service.write.db.DeleteSubDocumentArrayQueryBuilder;
import io.stargate.sgv2.docsapi.service.write.db.DeleteSubDocumentKeysQueryBuilder;
import io.stargate.sgv2.docsapi.service.write.db.DeleteSubDocumentPathQueryBuilder;
import io.stargate.sgv2.docsapi.service.write.db.InsertQueryBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class DocumentWriteService {

  private final StargateRequestInfo requestInfo;
  private final TimeSource timeSource;
  private final InsertQueryBuilder insertQueryBuilder;
  private final boolean useLoggedBatches;
  private final boolean treatBooleansAsNumeric;
  private final DocumentProperties documentProperties;
  private final QueriesConfig queriesConfig;

  @Inject
  public DocumentWriteService(
      StargateRequestInfo requestInfo,
      TimeSource timeSource,
      DataStoreProperties dataStoreProperties,
      DocumentProperties documentProperties,
      QueriesConfig queriesConfig) {
    this.requestInfo = requestInfo;
    this.insertQueryBuilder = new InsertQueryBuilder(documentProperties);
    this.timeSource = timeSource;
    this.useLoggedBatches = dataStoreProperties.loggedBatchesEnabled();
    this.treatBooleansAsNumeric = dataStoreProperties.treatBooleansAsNumeric();
    this.documentProperties = documentProperties;
    this.queriesConfig = queriesConfig;
  }

  /**
   * Writes a single document, without deleting the existing document with the same key. Please call
   * this method only if you are sure that the document with given ID does not exist.
   *
   * @param keyspace Keyspace to store document in.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param rows Rows of this document.
   * @param ttl the time-to-live of the rows (seconds)
   * @param context Execution content for profiling.
   * @return Uni containing the {@link ResultSet} of the batch execution.
   */
  @WithSpan
  public Uni<ResultSet> writeDocument(
      String keyspace,
      String collection,
      String documentId,
      List<JsonShreddedRow> rows,
      Integer ttl,
      ExecutionContext context) {

    return Uni.createFrom()
        .item(() -> insertQueryBuilder.buildQuery(keyspace, collection, ttl))
        .map(
            query -> {
              long timestamp = timeSource.currentTimeMicros();
              return rows.stream()
                  .map(
                      row ->
                          insertQueryBuilder.bind(
                              query, documentId, row, ttl, timestamp, treatBooleansAsNumeric))
                  .toList();
            })
        .flatMap(
            boundQueries ->
                executeBatch(
                    requestInfo.getStargateBridge(), boundQueries, context.nested("ASYNC INSERT")));
  }

  /**
   * Updates a single document, ensuring that existing document with the same key will be deleted
   * first.
   *
   * @param keyspace Keyspace to store document in.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param rows Rows of this document.
   * @param ttl the time-to-live of the rows (seconds)
   * @param context Execution content for profiling.
   * @return Uni containing the {@link ResultSet} of the batch execution.
   */
  @WithSpan
  public Uni<ResultSet> updateDocument(
      String keyspace,
      String collection,
      String documentId,
      List<JsonShreddedRow> rows,
      Integer ttl,
      ExecutionContext context) {
    List<String> subDocumentPath = Collections.emptyList();
    return updateDocumentInternal(
        keyspace, collection, documentId, subDocumentPath, rows, ttl, context);
  }

  /**
   * Updates a single document, ensuring that existing document with the same key have the given
   * sub-path deleted.
   *
   * @param keyspace Keyspace to store document in.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param subDocumentPath The sub-document path to delete.
   * @param rows Rows of this document.
   * @param ttl the time-to-live of the rows (seconds)
   * @param context Execution content for profiling.
   * @return Uni containing the {@link ResultSet} of the batch execution.
   */
  @WithSpan
  public Uni<ResultSet> updateDocument(
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      List<JsonShreddedRow> rows,
      Integer ttl,
      ExecutionContext context) {
    return updateDocumentInternal(
        keyspace, collection, documentId, subDocumentPath, rows, ttl, context);
  }

  private Uni<ResultSet> updateDocumentInternal(
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      List<JsonShreddedRow> rows,
      Integer ttl,
      ExecutionContext context) {

    return Uni.createFrom()
        .item(
            () -> {
              // check that sub path matches the rows supplied
              // TODO this smells like a bad design. Maybe not include sub-path during shredding,
              //  and instead add it here as a wrapper
              checkPathMatchesRows(subDocumentPath, rows);

              long timestamp = timeSource.currentTimeMicros();
              List<QueryOuterClass.Query> queries = new ArrayList<>(rows.size() + 1);

              // delete existing subpath
              AbstractDeleteQueryBuilder deleteQueryBuilder =
                  subDocumentPath.isEmpty()
                      ? new DeleteDocumentQueryBuilder(documentProperties)
                      : new DeleteSubDocumentPathQueryBuilder(
                          subDocumentPath, false, documentProperties);
              QueryOuterClass.Query deleteQuery =
                  deleteQueryBuilder.buildQuery(keyspace, collection);
              queries.add(deleteQueryBuilder.bind(deleteQuery, documentId, timestamp - 1));

              // then insert new one
              QueryOuterClass.Query insertQuery =
                  insertQueryBuilder.buildQuery(keyspace, collection, ttl);
              rows.forEach(
                  row ->
                      queries.add(
                          insertQueryBuilder.bind(
                              insertQuery,
                              documentId,
                              row,
                              ttl,
                              timestamp,
                              treatBooleansAsNumeric)));
              return queries;
            })
        .flatMap(
            boundQueries ->
                executeBatch(
                    requestInfo.getStargateBridge(), boundQueries, context.nested("ASYNC UPDATE")));
  }

  /**
   * Patches a single document at root, ensuring that:
   *
   * <ol>
   *   <li>If path currently matches an empty object, it will be removed
   *   <li>If path currently matches an existing JSON array, it will be removed
   *   <li>If path currently matches an existing JSON object, only patched keys will be removed
   * </ol>
   *
   * Note that this method does not allow array patching.
   *
   * @param keyspace Keyspace to store document in.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param rows Rows of the patch.
   * @param ttl the time-to-live of the rows (seconds)
   * @param context Execution content for profiling.
   * @return Uni containing the {@link ResultSet} of the batch execution.
   */
  @WithSpan
  public Uni<ResultSet> patchDocument(
      String keyspace,
      String collection,
      String documentId,
      List<JsonShreddedRow> rows,
      Integer ttl,
      ExecutionContext context) {
    List<String> path = Collections.emptyList();
    return patchDocumentInternal(keyspace, collection, documentId, path, rows, ttl, context);
  }

  /**
   * Patches a single document at given sub-path, ensuring that:
   *
   * <ol>
   *   <li>If path currently matches an empty object, it will be removed
   *   <li>If path currently matches an existing JSON array, it will be removed
   *   <li>If path currently matches an existing JSON object, only patched keys will be removed
   * </ol>
   *
   * Note that this method does not allow array patching.
   *
   * @param keyspace Keyspace to store document in.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param subDocumentPath The sub-document path to patch. Empty patches at the root level.
   * @param rows Rows of the patch.
   * @param ttl the time-to-live of the rows (seconds)
   * @param context Execution content for profiling.
   * @return Uni containing the {@link ResultSet} of the batch execution.
   */
  @WithSpan
  public Uni<ResultSet> patchDocument(
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      List<JsonShreddedRow> rows,
      Integer ttl,
      ExecutionContext context) {
    return patchDocumentInternal(
        keyspace, collection, documentId, subDocumentPath, rows, ttl, context);
  }

  private Uni<ResultSet> patchDocumentInternal(
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      List<JsonShreddedRow> rows,
      Integer ttl,
      ExecutionContext context) {

    return Uni.createFrom()
        .item(
            () -> {
              // check that sub path matches the rows supplied
              // TODO this smells like a bad design. Maybe not include sub-path during shredding,
              //  and instead add it here as a wrapper
              checkPathMatchesRows(subDocumentPath, rows);

              long timestamp = timeSource.currentTimeMicros();
              List<QueryOuterClass.Query> queries = new ArrayList<>(rows.size() + 3);

              // Example: we are patching the object {"d": 2, "e": 3} at path ["b","c"]

              // If the existing document already contains an object at this subpath, delete all the
              // keys that we are about to patch, for example:
              // {"a": 1, "b": {"c": {d": 3, "f": 4}}} => {"a": 1, "b": {"c": {"f": 4}}}
              List<String> patchedKeys = firstLevelPatchedKeys(subDocumentPath, rows);
              DeleteSubDocumentKeysQueryBuilder deletePatchedKeysBuilder =
                  new DeleteSubDocumentKeysQueryBuilder(
                      subDocumentPath, patchedKeys, documentProperties);
              QueryOuterClass.Query deletePatchedKeysQuery =
                  deletePatchedKeysBuilder.buildQuery(keyspace, collection);
              queries.add(
                  deletePatchedKeysBuilder.bind(deletePatchedKeysQuery, documentId, timestamp - 1));

              // If the existing document contains a primitive, empty array or empty object at this
              // subpath, delete it, for example:
              // {"a": 1, "b": {"c": 2, "f": 4}} => {"a": 1, "b": {"f": 4}}
              DeleteSubDocumentPathQueryBuilder deleteExactPathBuilder =
                  new DeleteSubDocumentPathQueryBuilder(subDocumentPath, true, documentProperties);
              QueryOuterClass.Query deleteExactPathQuery =
                  deleteExactPathBuilder.buildQuery(keyspace, collection);
              queries.add(
                  deleteExactPathBuilder.bind(deleteExactPathQuery, documentId, timestamp - 1));

              // If the existing document contains a non-empty array at this subpath, delete it, for
              // example:
              // {"a": 1, "b": {"c": [1,2,3], "f": 4}} => {"a": 1, "b": {"f": 4}}
              DeleteSubDocumentArrayQueryBuilder deleteArrayBuilder =
                  new DeleteSubDocumentArrayQueryBuilder(subDocumentPath, documentProperties);
              QueryOuterClass.Query deleteArrayQuery =
                  deleteArrayBuilder.buildQuery(keyspace, collection);
              queries.add(deleteArrayBuilder.bind(deleteArrayQuery, documentId, timestamp - 1));

              // Finally, insert the new data.
              QueryOuterClass.Query insertQuery =
                  insertQueryBuilder.buildQuery(keyspace, collection, ttl);
              rows.forEach(
                  row ->
                      queries.add(
                          insertQueryBuilder.bind(
                              insertQuery,
                              documentId,
                              row,
                              ttl,
                              timestamp,
                              treatBooleansAsNumeric)));

              return queries;
            })
        .flatMap(
            boundQueries ->
                executeBatch(
                    requestInfo.getStargateBridge(), boundQueries, context.nested("ASYNC PATCH")));
  }

  /**
   * Deletes a single whole document.
   *
   * @param keyspace Keyspace to delete a document from.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param context Execution content for profiling.
   * @return Uni containing the {@link ResultSet} of the batch execution.
   */
  @WithSpan
  public Uni<ResultSet> deleteDocument(
      String keyspace, String collection, String documentId, ExecutionContext context) {
    List<String> subDocumentPath = Collections.emptyList();
    return deleteDocumentInternal(keyspace, collection, documentId, subDocumentPath, context);
  }

  /**
   * Deletes a single (sub-)document, ensuring that existing document with the same key have the
   * given sub-path deleted.
   *
   * @param keyspace Keyspace to delete a document from.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param subDocumentPath The sub-document path to delete.
   * @param context Execution content for profiling.
   * @return Single containing the {@link ResultSet} of the batch execution.
   */
  @WithSpan
  public Uni<ResultSet> deleteDocument(
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      ExecutionContext context) {
    return deleteDocumentInternal(keyspace, collection, documentId, subDocumentPath, context);
  }

  private Uni<ResultSet> deleteDocumentInternal(
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      ExecutionContext context) {

    return Uni.createFrom()
        .item(
            () -> {
              long timestamp = timeSource.currentTimeMicros();
              AbstractDeleteQueryBuilder deleteQueryBuilder =
                  subDocumentPath.isEmpty()
                      ? new DeleteDocumentQueryBuilder(documentProperties)
                      : new DeleteSubDocumentPathQueryBuilder(
                          subDocumentPath, false, documentProperties);
              QueryOuterClass.Query deleteQuery =
                  deleteQueryBuilder.buildQuery(keyspace, collection);
              return deleteQueryBuilder.bind(deleteQuery, documentId, timestamp);
            })
        .flatMap(
            query ->
                executeSingle(
                    requestInfo.getStargateBridge(), query, context.nested("ASYNC DELETE")));
  }

  private Uni<ResultSet> executeBatch(
      StargateBridge bridge, List<QueryOuterClass.Query> boundQueries, ExecutionContext context) {

    // trace queries in context
    boundQueries.forEach(context::traceDeferredDml);

    // then execute batch
    Batch.Type type = useLoggedBatches ? Batch.Type.LOGGED : Batch.Type.UNLOGGED;
    Batch.Builder batch =
        Batch.newBuilder()
            .setType(type)
            .setParameters(
                QueryOuterClass.BatchParameters.newBuilder()
                    .setConsistency(
                        QueryOuterClass.ConsistencyValue.newBuilder()
                            .setValue(queriesConfig.consistency().writes())));
    boundQueries.forEach(
        query ->
            batch.addQueries(
                QueryOuterClass.BatchQuery.newBuilder()
                    .setCql(query.getCql())
                    .setValues(query.getValues())));

    return bridge.executeBatch(batch.build()).map(QueryOuterClass.Response::getResultSet);
  }

  private Uni<ResultSet> executeSingle(
      StargateBridge bridge, QueryOuterClass.Query boundQuery, ExecutionContext context) {

    context.traceDeferredDml(boundQuery);

    boundQuery =
        QueryOuterClass.Query.newBuilder(boundQuery)
            .setParameters(
                QueryOuterClass.QueryParameters.newBuilder()
                    .setConsistency(
                        QueryOuterClass.ConsistencyValue.newBuilder()
                            .setValue(queriesConfig.consistency().writes())))
            .build();

    return bridge.executeQuery(boundQuery).map(QueryOuterClass.Response::getResultSet);
  }

  // makes sure that any row starts with the given sub-document path
  private void checkPathMatchesRows(List<String> subDocumentPath, List<JsonShreddedRow> rows) {
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
  }

  // collects the first level patched keys
  private List<String> firstLevelPatchedKeys(
      List<String> subDocumentPath, List<JsonShreddedRow> rows) {
    List<String> keys =
        rows.stream()
            // here we need the first key after the sub-document path
            // protect against empty object patching
            .filter(row -> row.getPath().size() > subDocumentPath.size())
            // we need to ensure arrays are not allowed
            .map(
                row -> {
                  String key = row.getPath().get(subDocumentPath.size());
                  if (DocsApiUtils.isArrayPath(key)) {
                    throw new ErrorCodeRuntimeException(
                        ErrorCode.DOCS_API_PATCH_ARRAY_NOT_ACCEPTED);
                  }
                  return key;
                })
            .distinct()
            .collect(Collectors.toList());

    // make sure it's not empty, at least one key needed if on root doc
    if (keys.isEmpty() && subDocumentPath.isEmpty()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_PATCH_EMPTY_NOT_ACCEPTED);
    }

    return keys;
  }
}
