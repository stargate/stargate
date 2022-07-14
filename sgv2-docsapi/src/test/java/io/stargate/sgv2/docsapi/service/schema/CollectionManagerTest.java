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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.BridgeTest;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.grpc.GrpcClients;
import io.stargate.sgv2.docsapi.service.schema.query.CollectionQueryProvider;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
class CollectionManagerTest extends BridgeTest {

  @Inject CollectionManager collectionManager;

  @Inject CollectionQueryProvider queryProvider;

  @Inject DocumentProperties documentProperties;

  @Inject GrpcClients grpcClients;

  @InjectMock StargateRequestInfo requestInfo;

  ArgumentCaptor<QueryOuterClass.Query> queryCaptor;

  @BeforeEach
  public void init() {
    queryCaptor = ArgumentCaptor.forClass(QueryOuterClass.Query.class);
    doAnswer(invocation -> grpcClients.bridgeClient(Optional.empty(), Optional.empty()))
        .when(requestInfo)
        .getStargateBridge();
  }

  Schema.CqlKeyspaceDescribe.Builder getValidTableAndKeyspaceBuilder(
      String namespace, String collection) {
    QueryOuterClass.ColumnSpec.Builder partitionColumn =
        QueryOuterClass.ColumnSpec.newBuilder()
            .setName(documentProperties.tableProperties().keyColumnName());
    Set<QueryOuterClass.ColumnSpec> clusteringColumns =
        documentProperties.tableColumns().pathColumnNames().stream()
            .map(c -> QueryOuterClass.ColumnSpec.newBuilder().setName(c).build())
            .collect(Collectors.toSet());
    Set<QueryOuterClass.ColumnSpec> valueColumns =
        documentProperties.tableColumns().valueColumnNames().stream()
            .map(c -> QueryOuterClass.ColumnSpec.newBuilder().setName(c).build())
            .collect(Collectors.toSet());

    Schema.CqlTable.Builder table =
        Schema.CqlTable.newBuilder()
            .setName(collection)
            .addPartitionKeyColumns(partitionColumn)
            .addAllClusteringKeyColumns(clusteringColumns)
            .addAllColumns(valueColumns);
    return Schema.CqlKeyspaceDescribe.newBuilder()
        .setCqlKeyspace(Schema.CqlKeyspace.newBuilder().setName(namespace))
        .addTables(table);
  }

  Schema.CqlKeyspaceDescribe getValidTableAndKeyspace(String namespace, String collection) {
    return getValidTableAndKeyspaceBuilder(namespace, collection).build();
  }

  @Nested
  class GetValidCollectionTable {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      Schema.CqlKeyspaceDescribe keyspace = getValidTableAndKeyspace(namespace, collection);

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspace);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlTable> result =
          collectionManager
              .getValidCollectionTable(namespace, collection)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(keyspace.getTables(0)).assertCompleted();

      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }

    @Test
    public void notValid() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      Schema.CqlTable.Builder table = Schema.CqlTable.newBuilder().setName(collection);
      Schema.CqlKeyspaceDescribe keyspace =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(Schema.CqlKeyspace.newBuilder().setName(namespace))
              .addTables(table)
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspace);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlTable> result =
          collectionManager
              .getValidCollectionTable(namespace, collection)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      Throwable failure = result.awaitFailure().getFailure();
      assertThat(failure)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCode.DOCS_API_GENERAL_TABLE_NOT_A_COLLECTION);

      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }

    @Test
    public void noTable() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      Schema.CqlKeyspaceDescribe keyspace =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(Schema.CqlKeyspace.newBuilder().setName(namespace))
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspace);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlTable> result =
          collectionManager
              .getValidCollectionTable(namespace, collection)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      Throwable failure = result.awaitFailure().getFailure();
      assertThat(failure)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DATASTORE_TABLE_DOES_NOT_EXIST);

      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }

    @Test
    public void noKeyspace() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                Status status = Status.NOT_FOUND;
                observer.onError(new StatusRuntimeException(status));
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlTable> result =
          collectionManager
              .getValidCollectionTable(namespace, collection)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      Throwable failure = result.awaitFailure().getFailure();
      assertThat(failure)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DATASTORE_KEYSPACE_DOES_NOT_EXIST);

      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }
  }

  @Nested
  class GetValidCollectionTables {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      Schema.CqlKeyspaceDescribe keyspace =
          getValidTableAndKeyspaceBuilder(namespace, collection)
              .addTables(Schema.CqlTable.newBuilder().setName("other").build())
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspace);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      List<Schema.CqlTable> result =
          collectionManager
              .getValidCollectionTables(namespace)
              .subscribe()
              .withSubscriber(AssertSubscriber.create(2))
              .awaitItems(1)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      assertThat(result).singleElement().isEqualTo(keyspace.getTables(0));

      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }
  }

  @Nested
  class EnsureValidCollectionTable {

    @Test
    public void alreadyExists() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      Schema.CqlKeyspaceDescribe keyspace = getValidTableAndKeyspace(namespace, collection);

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspace);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      Schema.CqlTable result =
          collectionManager
              .ensureValidDocumentTable(namespace, collection)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result).isNotNull();

      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }

    @Test
    public void created() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      Schema.CqlKeyspaceDescribe keyspace =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(Schema.CqlKeyspace.newBuilder().setName(namespace))
              .build();

      Schema.CqlKeyspaceDescribe validTableAndKeyspace =
          getValidTableAndKeyspace(namespace, collection);

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspace);
                observer.onCompleted();
                return null;
              })
          .doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(validTableAndKeyspace);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      QueryOuterClass.Response response =
          QueryOuterClass.Response.newBuilder()
              .setSchemaChange(
                  QueryOuterClass.SchemaChange.newBuilder()
                      .setChangeType(QueryOuterClass.SchemaChange.Type.CREATED)
                      .build())
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      Schema.CqlTable result =
          collectionManager
              .ensureValidDocumentTable(namespace, collection)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result).isNotNull();

      verify(bridgeService, times(2)).describeKeyspace(any(), any());
      verify(bridgeService, times(5)).executeQuery(queryCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(queryCaptor.getAllValues())
          .hasSize(5)
          .contains(queryProvider.createCollectionQuery(namespace, collection))
          .containsAll(queryProvider.createCollectionIndexQueries(namespace, collection));
    }
  }

  @Nested
  class CreateCollectionTable {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      QueryOuterClass.Response response =
          QueryOuterClass.Response.newBuilder()
              .setSchemaChange(
                  QueryOuterClass.SchemaChange.newBuilder()
                      .setChangeType(QueryOuterClass.SchemaChange.Type.CREATED)
                      .build())
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      UniAssertSubscriber<Boolean> result =
          collectionManager
              .createCollectionTable(namespace, collection)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(true).assertCompleted();

      verify(bridgeService, times(5)).executeQuery(queryCaptor.capture(), any());
      assertThat(queryCaptor.getAllValues())
          .hasSize(5)
          .contains(queryProvider.createCollectionQuery(namespace, collection))
          .containsAll(queryProvider.createCollectionIndexQueries(namespace, collection));
    }

    @Test
    public void invalidName() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);

      UniAssertSubscriber<Boolean> result =
          collectionManager
              .createCollectionTable(namespace, "not-valid-name")
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      Throwable failure = result.awaitFailure().getFailure();
      assertThat(failure)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DATASTORE_TABLE_NAME_INVALID);

      verifyNoMoreInteractions(bridgeService);
    }
  }

  @Nested
  class DropCollectionTable {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      Schema.CqlKeyspaceDescribe keyspace = getValidTableAndKeyspace(namespace, collection);

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspace);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      QueryOuterClass.Response response =
          QueryOuterClass.Response.newBuilder()
              .setSchemaChange(
                  QueryOuterClass.SchemaChange.newBuilder()
                      .setChangeType(QueryOuterClass.SchemaChange.Type.DROPPED)
                      .build())
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      UniAssertSubscriber<Void> result =
          collectionManager
              .dropCollectionTable(namespace, collection)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(null).assertCompleted();

      verify(bridgeService).executeQuery(queryCaptor.capture(), any());
      assertThat(queryCaptor.getAllValues())
          .singleElement()
          .isEqualTo(queryProvider.deleteCollectionQuery(namespace, collection));
    }
  }

  @Nested
  class IsValidCollectionTable {

    @Test
    public void wrongPartitionSize() {
      Schema.CqlTable table = Schema.CqlTable.newBuilder().build();

      boolean result = collectionManager.isValidCollectionTable(table);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongPartitionColumnName() {
      Schema.CqlTable table =
          Schema.CqlTable.newBuilder()
              .addPartitionKeyColumns(
                  QueryOuterClass.ColumnSpec.newBuilder().setName("my-name").build())
              .build();

      boolean result = collectionManager.isValidCollectionTable(table);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongClusteringColumnsSize() {
      Schema.CqlTable table =
          Schema.CqlTable.newBuilder()
              .addPartitionKeyColumns(
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName(documentProperties.tableProperties().keyColumnName())
                      .build())
              .build();

      boolean result = collectionManager.isValidCollectionTable(table);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongClusteringColumnNames() {
      Schema.CqlTable table =
          Schema.CqlTable.newBuilder()
              .addPartitionKeyColumns(
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName(documentProperties.tableProperties().keyColumnName())
                      .build())
              .addAllClusteringKeyColumns(
                  IntStream.range(0, documentProperties.maxDepth())
                      .mapToObj(
                          i ->
                              QueryOuterClass.ColumnSpec.newBuilder()
                                  .setName(String.valueOf(i))
                                  .build())
                      .collect(Collectors.toSet()))
              .build();

      boolean result = collectionManager.isValidCollectionTable(table);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongValueColumnsSize() {
      DocumentTableProperties tableProperties = documentProperties.tableProperties();
      Schema.CqlTable table =
          Schema.CqlTable.newBuilder()
              .addPartitionKeyColumns(
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName(tableProperties.keyColumnName())
                      .build())
              .addAllClusteringKeyColumns(
                  IntStream.range(0, documentProperties.maxDepth())
                      .mapToObj(
                          i ->
                              QueryOuterClass.ColumnSpec.newBuilder()
                                  .setName(tableProperties.pathColumnName(i))
                                  .build())
                      .collect(Collectors.toSet()))
              .build();

      boolean result = collectionManager.isValidCollectionTable(table);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongValueColumnsNames() {
      DocumentTableProperties tableProperties = documentProperties.tableProperties();
      Schema.CqlTable table =
          Schema.CqlTable.newBuilder()
              .addPartitionKeyColumns(
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName(tableProperties.keyColumnName())
                      .build())
              .addAllClusteringKeyColumns(
                  IntStream.range(0, documentProperties.maxDepth())
                      .mapToObj(
                          i ->
                              QueryOuterClass.ColumnSpec.newBuilder()
                                  .setName(tableProperties.pathColumnName(i))
                                  .build())
                      .collect(Collectors.toSet()))
              .addColumns(
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName(tableProperties.leafColumnName())
                      .build())
              .addColumns(
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName(tableProperties.stringValueColumnName())
                      .build())
              .addColumns(
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName(tableProperties.booleanValueColumnName())
                      .build())
              .addColumns(
                  QueryOuterClass.ColumnSpec.newBuilder().setName("my-double-column").build())
              .build();

      boolean result = collectionManager.isValidCollectionTable(table);

      assertThat(result).isFalse();
    }
  }
}
