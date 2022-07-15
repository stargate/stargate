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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.BridgeTest;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.grpc.GrpcClients;
import io.stargate.sgv2.api.common.grpc.UnauthorizedTableException;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.schema.qualifier.Authorized;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
class AuthorizedCollectionManagerTest extends BridgeTest {

  // only one test to assert authorized schema fetch

  @Inject @Authorized CollectionManager collectionManager;

  @Inject DocumentProperties documentProperties;

  @Inject GrpcClients grpcClients;

  @InjectMock StargateRequestInfo requestInfo;

  ArgumentCaptor<QueryOuterClass.Query> queryCaptor;

  ArgumentCaptor<Schema.AuthorizeSchemaReadsRequest> schemaReadsCaptor;

  @BeforeEach
  public void init() {
    queryCaptor = ArgumentCaptor.forClass(QueryOuterClass.Query.class);
    schemaReadsCaptor = ArgumentCaptor.forClass(Schema.AuthorizeSchemaReadsRequest.class);
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
      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(true).build();
      Schema.CqlKeyspaceDescribe keyspace = getValidTableAndKeyspace(namespace, collection);

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.AuthorizeSchemaReadsResponse> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(authResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .authorizeSchemaReads(any(), any());

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

      collectionManager
          .getValidCollectionTable(namespace, collection)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(keyspace.getTables(0))
          .assertCompleted();

      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);

      // asert auth request
      assertThat(schemaReadsCaptor.getAllValues())
          .singleElement()
          .extracting(Schema.AuthorizeSchemaReadsRequest::getSchemaReadsList)
          .satisfies(
              reads ->
                  assertThat(reads)
                      .singleElement()
                      .satisfies(
                          read -> {
                            assertThat(read.getKeyspaceName()).isEqualTo(namespace);
                            assertThat(read.getElementName().getValue()).isEqualTo(collection);
                            assertThat(read.getElementType())
                                .isEqualTo(Schema.SchemaRead.ElementType.TABLE);
                          }));
    }

    @Test
    public void notAuthorized() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(false).build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.AuthorizeSchemaReadsResponse> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(authResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .authorizeSchemaReads(any(), any());

      collectionManager
          .getValidCollectionTable(namespace, collection)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure()
          .assertFailedWith(UnauthorizedTableException.class);

      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
    }
  }

  @Nested
  class GetValidCollectionTables {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String collection2 = RandomStringUtils.randomAlphanumeric(16);
      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder()
              .addAllAuthorized(Arrays.asList(true, true))
              .build();
      Schema.CqlKeyspaceDescribe keyspace =
          getValidTableAndKeyspaceBuilder(namespace, collection)
              .addTables(Schema.CqlTable.newBuilder().setName(collection2).build())
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.AuthorizeSchemaReadsResponse> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(authResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .authorizeSchemaReads(any(), any());

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

      collectionManager
          .getValidCollectionTables(namespace)
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitNextItem()
          .assertItems(keyspace.getTables(0))
          .awaitCompletion()
          .assertCompleted();

      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);

      // asert auth request
      assertThat(schemaReadsCaptor.getAllValues())
          .singleElement()
          .extracting(Schema.AuthorizeSchemaReadsRequest::getSchemaReadsList)
          .satisfies(
              reads ->
                  assertThat(reads)
                      .hasSize(2)
                      .anySatisfy(
                          read -> {
                            assertThat(read.getKeyspaceName()).isEqualTo(namespace);
                            assertThat(read.getElementName().getValue()).isEqualTo(collection);
                            assertThat(read.getElementType())
                                .isEqualTo(Schema.SchemaRead.ElementType.TABLE);
                          })
                      .anySatisfy(
                          read -> {
                            assertThat(read.getKeyspaceName()).isEqualTo(namespace);
                            assertThat(read.getElementName().getValue()).isEqualTo(collection2);
                            assertThat(read.getElementType())
                                .isEqualTo(Schema.SchemaRead.ElementType.TABLE);
                          }));
    }
  }
}
