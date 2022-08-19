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

import com.google.common.base.Splitter;
import io.opentelemetry.extension.annotations.WithSpan;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.QueriesConfig;
import io.stargate.sgv2.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import io.stargate.sgv2.docsapi.service.json.DeadLeaf;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class WriteBridgeService {

  // path splitter on dot
  private static final Splitter PATH_SPLITTER = Splitter.on(".");

  private final StargateRequestInfo requestInfo;
  private final TimeSource timeSource;
  private final InsertQueryBuilder insertQueryBuilder;
  private final boolean useLoggedBatches;
  private final boolean treatBooleansAsNumeric;
  private final DocumentProperties documentProperties;
  private final QueriesConfig queriesConfig;

  @Inject
  public WriteBridgeService(
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
        .item(
            () -> {
              long timestamp = timeSource.currentTimeMicros();
              return rows.stream()
                  .map(
                      row ->
                          insertQueryBuilder.buildAndBind(
                              keyspace,
                              collection,
                              ttl,
                              documentId,
                              row,
                              timestamp,
                              treatBooleansAsNumeric))
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
              List<QueryOuterClass.BatchQuery> queries = new ArrayList<>(rows.size() + 1);

              // delete existing subpath
              AbstractDeleteQueryBuilder deleteQueryBuilder =
                  subDocumentPath.isEmpty()
                      ? new DeleteDocumentQueryBuilder(documentProperties)
                      : new DeleteSubDocumentPathQueryBuilder(
                          subDocumentPath, false, documentProperties);
              queries.add(
                  deleteQueryBuilder.buildAndBind(keyspace, collection, documentId, timestamp - 1));

              // then insert new one
              rows.forEach(
                  row ->
                      queries.add(
                          insertQueryBuilder.buildAndBind(
                              keyspace,
                              collection,
                              ttl,
                              documentId,
                              row,
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
              List<QueryOuterClass.BatchQuery> queries = new ArrayList<>(rows.size() + 3);

              // Example: we are patching the object {"d": 2, "e": 3} at path ["b","c"]

              // If the existing document already contains an object at this subpath, delete all the
              // keys that we are about to patch, for example:
              // {"a": 1, "b": {"c": {d": 3, "f": 4}}} => {"a": 1, "b": {"c": {"f": 4}}}
              List<String> patchedKeys = firstLevelPatchedKeys(subDocumentPath, rows);
              queries.add(
                  new DeleteSubDocumentKeysQueryBuilder(
                          subDocumentPath, patchedKeys, documentProperties)
                      .buildAndBind(keyspace, collection, documentId, timestamp - 1));

              // If the existing document contains a primitive, empty array or empty object at this
              // subpath, delete it, for example:
              // {"a": 1, "b": {"c": 2, "f": 4}} => {"a": 1, "b": {"f": 4}}
              queries.add(
                  new DeleteSubDocumentPathQueryBuilder(subDocumentPath, true, documentProperties)
                      .buildAndBind(keyspace, collection, documentId, timestamp - 1));

              // If the existing document contains a non-empty array at this subpath, delete it, for
              // example:
              // {"a": 1, "b": {"c": [1,2,3], "f": 4}} => {"a": 1, "b": {"f": 4}}
              queries.add(
                  new DeleteSubDocumentArrayQueryBuilder(subDocumentPath, documentProperties)
                      .buildAndBind(keyspace, collection, documentId, timestamp - 1));

              // Finally, insert the new data.
              rows.forEach(
                  row ->
                      queries.add(
                          insertQueryBuilder.buildAndBind(
                              keyspace,
                              collection,
                              ttl,
                              documentId,
                              row,
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
   * Sets data at various paths on a single document, relative to its root. This allows partial
   * updates of any data in a document, without touching unrelated data.
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
  public Uni<ResultSet> setPathsOnDocument(
      String keyspace,
      String collection,
      String documentId,
      List<JsonShreddedRow> rows,
      Integer ttl,
      ExecutionContext context) {
    return Uni.createFrom()
        .item(
            () -> {
              long timestamp = timeSource.currentTimeMicros();
              Set<List<String>> distinctDocumentPaths = new HashSet<>();
              rows.forEach(row -> distinctDocumentPaths.add(row.getPath()));
              List<QueryOuterClass.BatchQuery> queries = new ArrayList<>(rows.size() + 3);

              // For each distinct path that is going to be set, delete the exact path first.
              distinctDocumentPaths.forEach(
                  path ->
                      queries.add(
                          new DeleteSubDocumentPathQueryBuilder(path, true, documentProperties)
                              .buildAndBind(keyspace, collection, documentId, timestamp - 1)));

              // Finally, insert the new data.
              rows.forEach(
                  row ->
                      queries.add(
                          insertQueryBuilder.buildAndBind(
                              keyspace,
                              collection,
                              ttl,
                              documentId,
                              row,
                              timestamp,
                              treatBooleansAsNumeric)));

              return queries;
            })
        .flatMap(
            boundQueries ->
                executeBatch(
                    requestInfo.getStargateBridge(), boundQueries, context.nested("ASYNC SET")));
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
              return deleteQueryBuilder.buildAndBind(keyspace, collection, documentId, timestamp);
            })
        .flatMap(
            query ->
                executeSingle(
                    requestInfo.getStargateBridge(), query, context.nested("ASYNC DELETE")));
  }

  /**
   * Deletes a given dead leaves for a document.
   *
   * @param keyspace Keyspace to delete a dead leaves from.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param microsTimestamp Micros timestamp to use in delete queries.
   * @param deadLeaves A map of JSON paths (f.e. $.some.path) to dead leaves to delete.
   * @param context Execution content for profiling.
   * @return Single containing the {@link ResultSet} of the batch execution.
   */
  public Uni<ResultSet> deleteDeadLeaves(
      String keyspace,
      String collection,
      String documentId,
      long microsTimestamp,
      Map<String, Set<DeadLeaf>> deadLeaves,
      ExecutionContext context) {
    return Uni.createFrom()
        .item(
            () -> {
              List<QueryOuterClass.BatchQuery> queries = new ArrayList<>();
              for (Map.Entry<String, Set<DeadLeaf>> entry : deadLeaves.entrySet()) {
                String path = entry.getKey();
                Set<DeadLeaf> leaves = entry.getValue();

                getDeadLeavesQueryBuilders(path, leaves)
                    .forEach(
                        builder -> {
                          queries.add(
                              builder.buildAndBind(
                                  keyspace, collection, documentId, microsTimestamp));
                        });
              }
              return queries;
            })
        .flatMap(
            boundQueries ->
                executeBatch(
                    requestInfo.getStargateBridge(),
                    boundQueries,
                    context.nested("ASYNC DOCUMENT CORRECTION")));
  }

  // creates needed query builders for one path of dead leaves
  private List<AbstractDeleteQueryBuilder> getDeadLeavesQueryBuilders(
      String path, Set<DeadLeaf> leaves) {

    // path is expected as $.path.field
    List<String> pathParts = PATH_SPLITTER.splitToList(path);
    if (pathParts.isEmpty()) {
      return Collections.emptyList();
    }

    // we remove the first one ($)
    List<String> pathToDelete = pathParts.subList(1, pathParts.size());
    boolean deleteArray = false;
    boolean deleteAll = false;
    List<String> keysToDelete = new ArrayList<>(leaves.size());
    for (DeadLeaf deadLeaf : leaves) {
      if (DeadLeaf.STAR_LEAF.equals(deadLeaf)) {
        deleteAll = true;
      } else if (DeadLeaf.ARRAY_LEAF.equals(deadLeaf)) {
        deleteArray = true;
      } else {
        keysToDelete.add(deadLeaf.getName());
      }
    }

    List<AbstractDeleteQueryBuilder> builders = new ArrayList<>();

    // in case of delete all, just that one
    if (deleteAll) {
      DeleteSubDocumentPathQueryBuilder deleteSubDocumentPathQueryBuilder =
          new DeleteSubDocumentPathQueryBuilder(pathToDelete, false, documentProperties);
      builders.add(deleteSubDocumentPathQueryBuilder);
    } else {
      // otherwise if we have any keys include that
      // if it's delete array include array deletion as well

      if (!keysToDelete.isEmpty()) {
        DeleteSubDocumentKeysQueryBuilder deleteSubDocumentKeysQueryBuilder =
            new DeleteSubDocumentKeysQueryBuilder(pathToDelete, keysToDelete, documentProperties);
        builders.add(deleteSubDocumentKeysQueryBuilder);
      }

      if (deleteArray) {
        DeleteSubDocumentArrayQueryBuilder deleteSubDocumentArrayQueryBuilder =
            new DeleteSubDocumentArrayQueryBuilder(pathToDelete, documentProperties);
        builders.add(deleteSubDocumentArrayQueryBuilder);
      }
    }

    return builders;
  }

  private Uni<ResultSet> executeBatch(
      StargateBridge bridge,
      List<QueryOuterClass.BatchQuery> batchQueries,
      ExecutionContext context) {

    // trace queries in context
    batchQueries.forEach(q -> context.traceDeferredDml(q.getCql()));

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
    batchQueries.forEach(batch::addQueries);

    return bridge.executeBatch(batch.build()).map(QueryOuterClass.Response::getResultSet);
  }

  private Uni<ResultSet> executeSingle(
      StargateBridge bridge, QueryOuterClass.BatchQuery batchQuery, ExecutionContext context) {

    context.traceDeferredDml(batchQuery.getCql());

    QueryOuterClass.Query singleQuery =
        QueryOuterClass.Query.newBuilder()
            .setCql(batchQuery.getCql())
            .setValues(batchQuery.getValues())
            .setParameters(
                QueryOuterClass.QueryParameters.newBuilder()
                    .setConsistency(
                        QueryOuterClass.ConsistencyValue.newBuilder()
                            .setValue(queriesConfig.consistency().writes())))
            .build();

    return bridge.executeQuery(singleQuery).map(QueryOuterClass.Response::getResultSet);
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
