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
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.cql.builder.Replication;
import io.stargate.sgv2.api.common.grpc.UnauthorizedKeyspaceException;
import io.stargate.sgv2.common.bridge.BridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.schema.query.NamespaceQueryProvider;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class NamespaceManagerTest extends BridgeTest {

  @Inject NamespaceManager namespaceManager;

  @Inject NamespaceQueryProvider queryProvider;

  @GrpcClient("bridge")
  StargateBridge bridge;

  @InjectMock StargateRequestInfo requestInfo;

  ArgumentCaptor<QueryOuterClass.Query> queryCaptor;

  @BeforeEach
  public void init() {
    queryCaptor = ArgumentCaptor.forClass(QueryOuterClass.Query.class);
    doAnswer(invocation -> bridge).when(requestInfo).getStargateBridge();
  }

  @Nested
  class GetNamespace {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspaceDescribe keyspace = Schema.CqlKeyspaceDescribe.newBuilder().build();

      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(true).build();

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

      namespaceManager
          .getNamespace(namespace)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(keyspace)
          .assertCompleted();

      verify(bridgeService).authorizeSchemaReads(any(), any());
      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }

    @Test
    public void notExisting() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);

      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(true).build();

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
                observer.onError(new StatusRuntimeException(Status.NOT_FOUND));
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      Throwable failure =
          namespaceManager
              .getNamespace(namespace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(failure)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DATASTORE_KEYSPACE_DOES_NOT_EXIST);

      verify(bridgeService).authorizeSchemaReads(any(), any());
      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }

    @Test
    public void notAuthorized() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);

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

      Throwable failure =
          namespaceManager
              .getNamespace(namespace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(failure).isInstanceOf(UnauthorizedKeyspaceException.class);

      verify(bridgeService).authorizeSchemaReads(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }
  }

  @Nested
  class GetNamespaces {

    @Test
    public void happyPath() {
      String keyspace1 = RandomStringUtils.randomAlphanumeric(16);
      String keyspace2 = RandomStringUtils.randomAlphanumeric(16);
      QueryOuterClass.ResultSet.Builder resultSet =
          QueryOuterClass.ResultSet.newBuilder()
              .addRows(QueryOuterClass.Row.newBuilder().addValues(Values.of(keyspace1)).build())
              .addRows(QueryOuterClass.Row.newBuilder().addValues(Values.of(keyspace2)).build());
      QueryOuterClass.Response queryResponse =
          QueryOuterClass.Response.newBuilder().setResultSet(resultSet).build();

      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder()
              .addAllAuthorized(Arrays.asList(true, false))
              .build();

      doAnswer(
              invocationOnMock -> {
                Schema.DescribeKeyspaceQuery query = invocationOnMock.getArgument(0);
                String keyspace = query.getKeyspaceName();
                Schema.CqlKeyspace value =
                    Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
                Schema.CqlKeyspaceDescribe cqlKeyspace =
                    Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(value).build();
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(cqlKeyspace);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

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
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(queryResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      List<Schema.CqlKeyspaceDescribe> result =
          namespaceManager
              .getNamespaces()
              .subscribe()
              .withSubscriber(AssertSubscriber.create())
              .awaitNextItems(1)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      verify(bridgeService).executeQuery(queryCaptor.capture(), any());
      verify(bridgeService).authorizeSchemaReads(any(), any());
      verify(bridgeService, times(1)).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert result
      assertThat(result)
          .hasSize(1)
          .singleElement()
          .satisfies(k -> assertThat(k.getCqlKeyspace().getName()).isEqualTo(keyspace1));
    }

    @Test
    public void noneExists() {
      QueryOuterClass.ResultSet.Builder resultSet = QueryOuterClass.ResultSet.newBuilder();
      QueryOuterClass.Response queryResponse =
          QueryOuterClass.Response.newBuilder().setResultSet(resultSet).build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(queryResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      namespaceManager
          .getNamespaces()
          .subscribe()
          .withSubscriber(AssertSubscriber.create(1))
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

      verify(bridgeService).executeQuery(queryCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // only one query
      assertThat(queryCaptor.getAllValues())
          .singleElement()
          .satisfies(
              query ->
                  assertThat(query.getCql())
                      .isEqualTo("SELECT keyspace_name FROM system_schema.keyspaces"));
    }
  }

  @Nested
  class CreateNamespace {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      Replication replication = Replication.simpleStrategy(1);

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

      namespaceManager
          .createNamespace(namespace, replication)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      verify(bridgeService).executeQuery(queryCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // only one query
      assertThat(queryCaptor.getAllValues())
          .singleElement()
          .isEqualTo(queryProvider.createNamespaceQuery(namespace, replication));
    }
  }

  @Nested
  class DropNamespace {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspaceDescribe keyspaceDescribe = Schema.CqlKeyspaceDescribe.newBuilder().build();

      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(true).build();

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

      QueryOuterClass.Response response =
          QueryOuterClass.Response.newBuilder()
              .setSchemaChange(
                  QueryOuterClass.SchemaChange.newBuilder()
                      .setChangeType(QueryOuterClass.SchemaChange.Type.DROPPED)
                      .build())
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspaceDescribe);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      namespaceManager
          .dropNamespace(namespace)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertCompleted();

      verify(bridgeService).authorizeSchemaReads(any(), any());
      verify(bridgeService).describeKeyspace(any(), any());
      verify(bridgeService).executeQuery(queryCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // only one query
      assertThat(queryCaptor.getAllValues())
          .singleElement()
          .isEqualTo(queryProvider.deleteNamespaceQuery(namespace));
    }

    @Test
    public void notExisting() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);

      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(true).build();

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
                observer.onError(new StatusRuntimeException(Status.NOT_FOUND));
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      Throwable failure =
          namespaceManager
              .dropNamespace(namespace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(failure)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DATASTORE_KEYSPACE_DOES_NOT_EXIST);

      verify(bridgeService).authorizeSchemaReads(any(), any());
      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }
  }
}
