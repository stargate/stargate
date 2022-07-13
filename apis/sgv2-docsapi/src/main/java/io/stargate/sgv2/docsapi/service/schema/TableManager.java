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

package io.stargate.sgv2.docsapi.service.schema;

import io.opentelemetry.extension.annotations.WithSpan;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableColumns;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.schema.query.CollectionQueryProvider;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Table manager provides basic operations on tables that are used for storing collections. */
@ApplicationScoped
public class TableManager {

  private static final Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>>
      MISSING_KEYSPACE_FUNCTION =
          keyspace -> {
            String message = "Unknown namespace %s, you must create it first.".formatted(keyspace);
            Exception exception =
                new ErrorCodeRuntimeException(ErrorCode.DATASTORE_KEYSPACE_DOES_NOT_EXIST, message);
            return Uni.createFrom().failure(exception);
          };

  @Inject DocumentProperties documentProperties;

  @Inject SchemaManager schemaManager;

  @Inject CollectionQueryProvider collectionQueryProvider;

  @Inject StargateRequestInfo requestInfo;

  /**
   * Fetches a table from the schema manager. Subclasses can override to use the authorized version.
   *
   * @param keyspaceName Keyspace
   * @param tableName Table
   * @return Table from schema manager
   */
  protected Uni<Schema.CqlTable> getTable(String keyspaceName, String tableName) {
    return schemaManager.getTable(keyspaceName, tableName, getMissingKeyspaceFailure());
  }

  /**
   * Fetches all tables of a keyspace from the schema manager. Subclasses can override to use the
   * authorized version.
   *
   * @param keyspaceName Keyspace
   * @return Multi of {@link io.stargate.bridge.proto.Schema.CqlTable}
   */
  protected Multi<Schema.CqlTable> getAllTables(String keyspaceName) {
    return schemaManager.getTables(keyspaceName, getMissingKeyspaceFailure());
  }

  /**
   * Function for the correct failure in case of the missing keyspace.
   *
   * @return Uni emitting failure with {@link ErrorCode#DATASTORE_KEYSPACE_DOES_NOT_EXIST}.
   */
  protected Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>>
      getMissingKeyspaceFailure() {
    return MISSING_KEYSPACE_FUNCTION;
  }

  /**
   * Creates a document table. Consider using the {@link #ensureValidDocumentTable(String, String)},
   * rather than calling this method.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Table name is not valid, with {@link ErrorCode#DATASTORE_TABLE_NAME_INVALID}
   * </ol>
   *
   * @param namespace Namespace of the collection.
   * @param collection Collection name.
   * @return True if table was created successfully.
   */
  @WithSpan
  public Uni<Boolean> createCollectionTable(String namespace, String collection) {
    // first check that table name is valid
    if (!collection.matches("^\\w+$")) {
      String message =
          "Could not create collection %s, it has invalid characters. Valid characters are alphanumeric and underscores."
              .formatted(collection);
      Exception exception =
          new ErrorCodeRuntimeException(ErrorCode.DATASTORE_TABLE_NAME_INVALID, message);
      return Uni.createFrom().failure(exception);
    }

    // get the client
    StargateBridge bridge = requestInfo.getStargateBridge();
    QueryOuterClass.Query request =
        collectionQueryProvider.createCollectionQuery(namespace, collection);

    // execute the request to create table
    return bridge
        .executeQuery(request)

        // TODO how to correctly inspect result
        //  should we go for ifNotExists, or react to failure here
        .map(QueryOuterClass.Response::getSchemaChange)
        .flatMap(
            schemaChange -> {

              // when table is create, go for the queries
              List<QueryOuterClass.Query> indexQueries =
                  collectionQueryProvider.createCollectionIndexQueries(namespace, collection);
              List<Uni<QueryOuterClass.Response>> indexQueryUnis =
                  indexQueries.stream().map(bridge::executeQuery).toList();

              // fire them in parallel, so we save time
              // TODO how to correctly handle failures result
              //  since index queries are ifNotExists, can we safely map to true here?
              return Uni.combine().all().unis(indexQueryUnis).combinedWith(results -> true);
            });
  }

  /**
   * Drops a document table.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Keyspace does not exists, with {@link ErrorCode#DATASTORE_KEYSPACE_DOES_NOT_EXIST}
   *   <li>Table does not exists, with {@link ErrorCode#DATASTORE_TABLE_DOES_NOT_EXIST}
   *   <li>Table is not valid, with {@link ErrorCode#DOCS_API_GENERAL_TABLE_NOT_A_COLLECTION}
   * </ol>
   *
   * @param namespace Namespace of the collection.
   * @param collection Collection name.
   * @return Void item in case table deletion was executed.
   */
  @WithSpan
  public Uni<Void> dropCollectionTable(String namespace, String collection) {
    // get table first
    return getValidCollectionTable(namespace, collection)

        // if exists and valid docs table drop
        .flatMap(
            table -> {
              StargateBridge bridge = requestInfo.getStargateBridge();
              QueryOuterClass.Query query =
                  collectionQueryProvider.deleteCollectionQuery(namespace, collection);

              // exec and return
              return bridge.executeQuery(query).map(any -> null);
            });
  }

  /**
   * Returns all valid collection tables from a given keyspace.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Keyspace does not exists, with {@link ErrorCode#DATASTORE_KEYSPACE_DOES_NOT_EXIST}
   * </ol>
   *
   * @param namespace Namespace.
   * @return Multi of {@link io.stargate.bridge.proto.Schema.CqlTable}s.
   */
  @WithSpan
  public Multi<Schema.CqlTable> getValidCollectionTables(String namespace) {
    // get all tables
    return getAllTables(namespace)

        // filter for valid collections
        .filter(this::isValidCollectionTable);
  }

  /**
   * Checks if the given table in a given keyspace is a valid collection table and returns it.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Keyspace does not exists, with {@link ErrorCode#DATASTORE_KEYSPACE_DOES_NOT_EXIST}
   *   <li>Table does not exists, with {@link ErrorCode#DATASTORE_TABLE_DOES_NOT_EXIST}
   *   <li>Table is not valid, with {@link ErrorCode#DOCS_API_GENERAL_TABLE_NOT_A_COLLECTION}
   * </ol>
   *
   * @param namespace Namespace of the collection.
   * @param collection Collection name.
   * @return True if table exists and is a valid document table, failure otherwise.
   */
  @WithSpan
  public Uni<Schema.CqlTable> getValidCollectionTable(String namespace, String collection) {
    // get the table
    return getValidDocumentTableInternal(namespace, collection)

        // if not then throw exception as not found
        .onItem()
        .ifNull()
        .switchTo(
            () -> {
              String msg = "Collection '%s' not found.".formatted(collection);
              Exception exception =
                  new ErrorCodeRuntimeException(ErrorCode.DATASTORE_TABLE_DOES_NOT_EXIST, msg);
              return Uni.createFrom().failure(exception);
            });
  }

  /**
   * Ensures the given table in a given keyspace is a valid collection table. If the table does not
   * exist, it will be created.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Keyspace does not exists, with {@link ErrorCode#DATASTORE_KEYSPACE_DOES_NOT_EXIST}
   *   <li>Table exists, but is not valid, with {@link
   *       ErrorCode#DOCS_API_GENERAL_TABLE_NOT_A_COLLECTION}
   * </ol>
   *
   * @param namespace Namespace of the collection.
   * @param collection Collection name.
   * @return True if table exists and is a valid document table, or it is created.
   */
  @WithSpan
  public Uni<Schema.CqlTable> ensureValidDocumentTable(String namespace, String collection) {
    // get the table
    return getValidDocumentTableInternal(namespace, collection)
        .onItem()
        .ifNull()
        .switchTo(
            () ->
                createCollectionTable(namespace, collection)
                    .onItem()
                    .transformToUni(
                        result -> getValidDocumentTableInternal(namespace, collection)));
  }

  // internal method for getting a valid document table
  private Uni<Schema.CqlTable> getValidDocumentTableInternal(
      String keyspaceName, String tableName) {
    // get the table
    return getTable(keyspaceName, tableName)

        // if found validate to ensure correctness
        .onItem()
        .ifNotNull()
        .transformToUni(
            table -> {
              if (isValidCollectionTable(table)) {
                return Uni.createFrom().item(table);
              } else {
                String format =
                    "The database table %s.%s is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted."
                        .formatted(keyspaceName, tableName);
                Exception exception =
                    new ErrorCodeRuntimeException(
                        ErrorCode.DOCS_API_GENERAL_TABLE_NOT_A_COLLECTION, format);
                return Uni.createFrom().failure(exception);
              }
            });
  }

  // checks that table contains expected columns
  // visible for testing
  boolean isValidCollectionTable(Schema.CqlTable table) {
    DocumentTableProperties tableProperties = documentProperties.tableProperties();
    DocumentTableColumns tableColumns = documentProperties.tableColumns();

    // partition columns
    // expect only one with the key column name
    List<QueryOuterClass.ColumnSpec> partitionKeyColumnsList = table.getPartitionKeyColumnsList();
    if (partitionKeyColumnsList.size() != 1) {
      return false;
    }
    if (!Objects.equals(
        partitionKeyColumnsList.get(0).getName(), tableProperties.keyColumnName())) {
      return false;
    }

    // clustering columns
    // expect max depth with path column names
    List<QueryOuterClass.ColumnSpec> clusteringKeyColumnsList = table.getClusteringKeyColumnsList();
    if (clusteringKeyColumnsList.size() != documentProperties.maxDepth()) {
      return false;
    }
    boolean missingPathColumn =
        clusteringKeyColumnsList.stream()
            .map(QueryOuterClass.ColumnSpec::getName)
            .anyMatch(c -> !tableColumns.pathColumnNames().contains(c));
    if (missingPathColumn) {
      return false;
    }

    // other columns
    if (table.getColumnsList().size() != tableColumns.valueColumnNames().size()) {
      return false;
    }
    boolean missingValueColumns =
        table.getColumnsList().stream()
            .map(QueryOuterClass.ColumnSpec::getName)
            .anyMatch(c -> !tableColumns.valueColumnNames().contains(c));

    return !missingValueColumns;
  }
}
