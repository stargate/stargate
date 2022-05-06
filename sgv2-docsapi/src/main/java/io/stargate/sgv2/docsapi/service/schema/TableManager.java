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

import io.smallrye.mutiny.Uni;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateBridge;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.grpc.GrpcClients;
import io.stargate.sgv2.docsapi.service.schema.common.SchemaManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class TableManager {

  @Inject DocumentProperties documentProperties;

  @Inject DataStoreProperties dataStoreProperties;

  @Inject SchemaManager schemaManager;

  @Inject GrpcClients grpcClients;

  @Inject StargateRequestInfo stargateRequestInfo;

  // TODO bridge instance to the request info
  //  tests

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
   * @param keyspaceName Keyspace name
   * @param tableName Table name
   * @return True if table was created successfully.
   */
  public Uni<Boolean> createDocumentTable(String keyspaceName, String tableName) {
    // first check that table name is valid
    if (!tableName.matches("^\\w+$")) {
      String message =
          String.format(
              "Could not create collection %s, it has invalid characters. Valid characters are alphanumeric and underscores.",
              tableName);
      Exception exception =
          new ErrorCodeRuntimeException(ErrorCode.DATASTORE_TABLE_NAME_INVALID, message);
      return Uni.createFrom().failure(exception);
    }

    // get the client
    StargateBridge bridge = grpcClients.bridgeClient(stargateRequestInfo);
    QueryOuterClass.Query request = createTableQuery(keyspaceName, tableName);

    // TODO can index queries be batched with create one?

    // execute the request to create table
    return bridge
        .executeQuery(request)

        // TODO how to correctly inspect result
        .map(QueryOuterClass.Response::getSchemaChange)
        .flatMap(
            schemaChange -> {

              // when table is create, go for the queries
              List<QueryOuterClass.Query> indexQueries =
                  createAllIndexQueries(keyspaceName, tableName);
              List<Uni<QueryOuterClass.Response>> indexQueryUnis =
                  indexQueries.stream().map(bridge::executeQuery).toList();

              // fire them in parallel, so we save time
              // TODO how to correctly handle failures result
              return Uni.combine().all().unis(indexQueryUnis).combinedWith(results -> true);
            });
  }

  /**
   * Drops a document table.
   *
   * @param keyspaceName Keyspace name
   * @param tableName Table name
   * @return True if table was deleted successfully.
   */
  public Uni<Boolean> dropDocumentTable(String keyspaceName, String tableName) {
    // get table first
    return isValidDocumentTable(keyspaceName, tableName)

        // if exists and valid docs table drop
        .flatMap(
            table -> {
              StargateBridge bridge = grpcClients.bridgeClient(stargateRequestInfo);

              // parameters for the local quorum
              // TODO this was not existing before in DocumentDB
              QueryOuterClass.QueryParameters parameters =
                  QueryOuterClass.QueryParameters.newBuilder()
                      .setConsistency(
                          QueryOuterClass.ConsistencyValue.newBuilder()
                              .setValue(QueryOuterClass.Consistency.LOCAL_QUORUM))
                      .build();

              // construct delete query
              QueryOuterClass.Query query =
                  new QueryBuilder()
                      .drop()
                      .table(keyspaceName, tableName)
                      .parameters(parameters)
                      .build();

              // exec and return
              return bridge.executeQuery(query).map(any -> true);
            });
  }

  /**
   * Checks if the given table in a given keyspace is a valid document table.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Keyspace does not exists, with {@link ErrorCode#DATASTORE_KEYSPACE_DOES_NOT_EXIST}
   *   <li>Table does not exists, with {@link ErrorCode#DATASTORE_TABLE_DOES_NOT_EXIST}
   *   <li>Table is not valid, with {@link ErrorCode#DOCS_API_GENERAL_TABLE_NOT_A_COLLECTION}
   * </ol>
   *
   * @param keyspaceName keyspace name
   * @param tableName table name
   * @return True if table exists and is a valid document table.
   */
  public Uni<Boolean> isValidDocumentTable(String keyspaceName, String tableName) {
    // get the table
    return getValidDocumentTableInternal(keyspaceName, tableName)

        // if emits the item, then fine
        .map(any -> true)

        // if not then throw exception as not found
        .onItem()
        .ifNull()
        .switchTo(
            () -> {
              String msg = String.format("Collection '%s' not found.", tableName);
              Exception exception =
                  new ErrorCodeRuntimeException(ErrorCode.DATASTORE_TABLE_DOES_NOT_EXIST, msg);
              return Uni.createFrom().failure(exception);
            });
  }

  /**
   * Checks if the given table in a given keyspace is a valid document table. If the table does not
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
   * @param keyspaceName keyspace name
   * @param tableName table name
   * @return True if table exists or is created, and is a valid document table.
   */
  public Uni<Boolean> ensureValidDocumentTable(String keyspaceName, String tableName) {
    // get the table
    return getValidDocumentTableInternal(keyspaceName, tableName)

        // if emits the item, then fine
        .map(any -> true)

        // if not then create the table
        .onItem()
        .ifNull()
        .switchTo(() -> createDocumentTable(keyspaceName, tableName).map(any -> true));
  }

  // internal method for getting a valid document table
  private Uni<Schema.CqlTable> getValidDocumentTableInternal(
      String keyspaceName, String tableName) {
    // get the table
    return getTable(keyspaceName, tableName)

        // if found validate to ensure correctness
        .flatMap(
            table -> {
              if (isValidDocumentTable(table)) {
                return Uni.createFrom().item(table);
              } else {
                String format =
                    String.format(
                        "The database table %s.%s is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted.",
                        keyspaceName, tableName);
                Exception exception =
                    new ErrorCodeRuntimeException(
                        ErrorCode.DOCS_API_GENERAL_TABLE_NOT_A_COLLECTION, format);
                return Uni.createFrom().failure(exception);
              }
            });
  }

  // gets the table from the schema manager, if exists
  private Uni<Schema.CqlTable> getTable(String keyspaceName, String tableName) {
    // get from schema manager
    return schemaManager
        .getKeyspace(keyspaceName)

        // if keyspace not found fail always
        .onItem()
        .ifNull()
        .switchTo(
            () -> {
              String message =
                  String.format("Unknown namespace %s, you must create it first.", keyspaceName);
              Exception exception =
                  new ErrorCodeRuntimeException(
                      ErrorCode.DATASTORE_KEYSPACE_DOES_NOT_EXIST, message);
              return Uni.createFrom().failure(exception);
            })

        // otherwise try to find the wanted table
        .flatMap(
            keyspace -> {
              List<Schema.CqlTable> tables = keyspace.getTablesList();
              Optional<Schema.CqlTable> table =
                  tables.stream().filter(t -> Objects.equals(t.getName(), tableName)).findFirst();
              return Uni.createFrom().optional(table);
            });
  }

  // constructs query for the creation of the document table
  private QueryOuterClass.Query createTableQuery(String keyspaceName, String tableName) {
    // all columns from the table props
    List<Column> columns = documentProperties.tableColumns().allColumns();

    // parameters for the local quorum
    // TODO this was not existing before in DocumentDB
    QueryOuterClass.QueryParameters parameters =
        QueryOuterClass.QueryParameters.newBuilder()
            .setConsistency(
                QueryOuterClass.ConsistencyValue.newBuilder()
                    .setValue(QueryOuterClass.Consistency.LOCAL_QUORUM))
            .build();

    // TODO can we utilize the ifNotExist
    QueryOuterClass.Query request =
        new QueryBuilder()
            .create()
            .table(keyspaceName, tableName)
            .column(columns)
            .parameters(parameters)
            .build();

    return request;
  }

  private List<QueryOuterClass.Query> createAllIndexQueries(String keyspaceName, String tableName) {
    DocumentTableProperties tableProperties = documentProperties.tableProperties();
    List<QueryOuterClass.Query> indexQueries = new ArrayList<>();

    if (dataStoreProperties.saiEnabled()) {
      indexQueries.add(
          createIndexQuery(
              keyspaceName, tableName, tableProperties.leafColumnName(), "StorageAttachedIndex"));
      indexQueries.add(
          createIndexQuery(
              keyspaceName,
              tableName,
              tableProperties.stringValueColumnName(),
              "StorageAttachedIndex"));
      indexQueries.add(
          createIndexQuery(
              keyspaceName,
              tableName,
              tableProperties.doubleValueColumnName(),
              "StorageAttachedIndex"));
      // SAI doesn't support booleans, so add a non-SAI index here.
      indexQueries.add(
          createIndexQuery(
              keyspaceName, tableName, tableProperties.booleanValueColumnName(), null));
    } else {
      indexQueries.add(
          createIndexQuery(keyspaceName, tableName, tableProperties.leafColumnName(), null));
      indexQueries.add(
          createIndexQuery(keyspaceName, tableName, tableProperties.stringValueColumnName(), null));
      indexQueries.add(
          createIndexQuery(keyspaceName, tableName, tableProperties.doubleValueColumnName(), null));
      indexQueries.add(
          createIndexQuery(
              keyspaceName, tableName, tableProperties.booleanValueColumnName(), null));
    }

    return indexQueries;
  }

  private QueryOuterClass.Query createIndexQuery(
      String keyspaceName, String tableName, String indexedColumn, String customIndexClass) {
    // TODO Do I need a quorum here as well?

    return new QueryBuilder()
        .create()
        .index()
        .ifNotExists()
        .on(keyspaceName, tableName)
        .column(indexedColumn)
        .custom(customIndexClass)
        .build();
  }

  // checks that table contains expected columns
  private Boolean isValidDocumentTable(Schema.CqlTable table) {
    // all expected columns
    String[] expectedColumns = documentProperties.tableColumns().allColumnNames();

    // collect all table columns
    Set<String> tableColumns =
        table.getColumnsList().stream()
            .map(QueryOuterClass.ColumnSpec::getName)
            .collect(Collectors.toSet());

    // if size don't match it's not the same
    // otherwise make sure they all are contained
    if (tableColumns.size() != expectedColumns.length) {
      return false;
    } else {
      // TODO expectedColumns can be provided as a set in TableColumns
      return tableColumns.containsAll(Arrays.asList(expectedColumns));
    }
  }
}
