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

package io.stargate.sgv2.api.common.optimistic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.protobuf.Int32Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.BridgeTest;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
class OptimisticQueryServiceTest extends BridgeTest {

  @Inject OptimisticQueryService service;

  @Inject SchemaManager schemaManager;

  @GrpcClient("bridge")
  StargateBridge bridge;

  @InjectMock StargateRequestInfo requestInfo;

  ArgumentCaptor<Schema.DescribeKeyspaceQuery> describeKeyspaceCaptor;

  ArgumentCaptor<Schema.AuthorizeSchemaReadsRequest> schemaReadsCaptor;

  ArgumentCaptor<Schema.QueryWithSchema> queryWithSchemaCaptor;

  @BeforeEach
  public void init() {
    describeKeyspaceCaptor = ArgumentCaptor.forClass(Schema.DescribeKeyspaceQuery.class);
    schemaReadsCaptor = ArgumentCaptor.forClass(Schema.AuthorizeSchemaReadsRequest.class);
    queryWithSchemaCaptor = ArgumentCaptor.forClass(Schema.QueryWithSchema.class);
    doAnswer(invocation -> bridge).when(requestInfo).getStargateBridge();
  }

  @Nested
  class ExecuteOptimistic {

    @Test
    public void happyPath() {
      // first invoke schema manager to be cached
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      int hash = RandomUtils.nextInt();
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable = Schema.CqlTable.newBuilder().setName(table).build();
      Schema.CqlKeyspaceDescribe keyspaceResponse =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .setHash(Int32Value.of(hash))
              .addTables(cqlTable)
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspaceResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> cache =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      cache.awaitItem().assertCompleted();

      // then call the optimistic query service
      QueryOuterClass.Response queryResponse =
          QueryOuterClass.Response.newBuilder().addWarnings("whatever").build();
      Schema.QueryWithSchemaResponse queryWithSchemaResponse =
          Schema.QueryWithSchemaResponse.newBuilder().setResponse(queryResponse).build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.QueryWithSchemaResponse> observer =
                    invocationOnMock.getArgument(1);

                observer.onNext(queryWithSchemaResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQueryWithSchema(any(), any());

      // mapping function
      Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryFunction =
          t -> {
            String cql = "SELECT FROM %s".formatted(t.getName());
            QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
            return Uni.createFrom().item(query);
          };

      UniAssertSubscriber<QueryOuterClass.Response> result =
          service
              .executeOptimistic(keyspace, table, k -> Uni.createFrom().nothing(), queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(queryResponse).assertCompleted();

      // verify bridge calls
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).executeQueryWithSchema(queryWithSchemaCaptor.capture(), any());
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));
      assertThat(queryWithSchemaCaptor.getAllValues())
          .allSatisfy(
              queryWithSchema -> {
                assertThat(queryWithSchema.getKeyspaceName()).isEqualTo(keyspace);
                assertThat(queryWithSchema.getKeyspaceHash()).isEqualTo(hash);
                assertThat(queryWithSchema.getQuery().getCql())
                    .isEqualTo("SELECT FROM %s".formatted(table));
              });
    }

    @Test
    public void keyspaceRemoved() {
      // first invoke schema manager to be cached
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      int hash = RandomUtils.nextInt();
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable = Schema.CqlTable.newBuilder().setName(table).build();
      Schema.CqlKeyspaceDescribe keyspaceResponse =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .setHash(Int32Value.of(hash))
              .addTables(cqlTable)
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspaceResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> cache =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      cache.awaitItem().assertCompleted();

      // then call the optimistic query service
      // set no keyspace as response
      Schema.QueryWithSchemaResponse queryWithSchemaResponse =
          Schema.QueryWithSchemaResponse.newBuilder()
              .setNoKeyspace(Schema.QueryWithSchemaResponse.NoKeyspace.newBuilder().build())
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.QueryWithSchemaResponse> observer =
                    invocationOnMock.getArgument(1);

                observer.onNext(queryWithSchemaResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQueryWithSchema(any(), any());

      // mapping function
      Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryFunction =
          t -> {
            String cql = "SELECT FROM %s".formatted(t.getName());
            QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
            return Uni.createFrom().item(query);
          };
      RuntimeException exception = new RuntimeException("my-exception");
      Function<String, Uni<? extends QueryOuterClass.Response>> missingKeyspace =
          k -> Uni.createFrom().failure(exception);

      UniAssertSubscriber<QueryOuterClass.Response> result =
          service
              .executeOptimistic(keyspace, table, missingKeyspace, queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      Throwable failure = result.awaitFailure().getFailure();
      assertThat(failure).isEqualTo(exception);

      // verify bridge calls
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).executeQueryWithSchema(queryWithSchemaCaptor.capture(), any());
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));
      assertThat(queryWithSchemaCaptor.getAllValues())
          .allSatisfy(
              queryWithSchema -> {
                assertThat(queryWithSchema.getKeyspaceName()).isEqualTo(keyspace);
                assertThat(queryWithSchema.getKeyspaceHash()).isEqualTo(hash);
                assertThat(queryWithSchema.getQuery().getCql())
                    .isEqualTo("SELECT FROM %s".formatted(table));
              });
    }

    @Test
    public void keyspaceUpdated() {
      // first invoke schema manager to be cached
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      int hash = RandomUtils.nextInt();
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable = Schema.CqlTable.newBuilder().setName(table).build();
      Schema.CqlKeyspaceDescribe keyspaceResponse =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .setHash(Int32Value.of(hash))
              .addTables(cqlTable)
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspaceResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> cache =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      cache.awaitItem().assertCompleted();

      // then call the optimistic query service
      // note that we will execute two calls in fact
      // first with updated keyspace as response
      // second as query executed
      int updatedHash = RandomUtils.nextInt();
      Schema.CqlKeyspace updatedCqlKeyspace =
          Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable updatedCqlTable =
          Schema.CqlTable.newBuilder()
              .setName(table)
              .addColumns(QueryOuterClass.ColumnSpec.newBuilder().setName("column").build())
              .build();
      Schema.CqlKeyspaceDescribe updatedKeyspace =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(updatedCqlKeyspace)
              .setHash(Int32Value.of(updatedHash))
              .addTables(updatedCqlTable)
              .build();
      Schema.QueryWithSchemaResponse updatedKeyspaceResponse =
          Schema.QueryWithSchemaResponse.newBuilder().setNewKeyspace(updatedKeyspace).build();

      QueryOuterClass.Response queryResponse =
          QueryOuterClass.Response.newBuilder().addWarnings("whatever").build();
      Schema.QueryWithSchemaResponse queryExecutedResponse =
          Schema.QueryWithSchemaResponse.newBuilder().setResponse(queryResponse).build();

      AtomicInteger callCount = new AtomicInteger(0);
      doAnswer(
              invocationOnMock -> {
                int call = callCount.getAndIncrement();
                StreamObserver<Schema.QueryWithSchemaResponse> observer =
                    invocationOnMock.getArgument(1);

                observer.onNext(call == 0 ? updatedKeyspaceResponse : queryExecutedResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQueryWithSchema(any(), any());

      // mapping function, we will add colum check to distinguish between first and second table
      Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryFunction =
          t -> {
            String col = t.getColumnsList().isEmpty() ? "*" : t.getColumnsList().get(0).getName();
            String cql = "SELECT %s FROM %s".formatted(col, t.getName());
            QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
            return Uni.createFrom().item(query);
          };

      UniAssertSubscriber<QueryOuterClass.Response> result =
          service
              .executeOptimistic(keyspace, table, k -> Uni.createFrom().nothing(), queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(queryResponse).assertCompleted();

      // verify bridge calls
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService, times(2))
          .executeQueryWithSchema(queryWithSchemaCaptor.capture(), any());
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));
      assertThat(queryWithSchemaCaptor.getAllValues())
          .hasSize(2)
          .anySatisfy(
              queryWithSchema -> {
                assertThat(queryWithSchema.getKeyspaceName()).isEqualTo(keyspace);
                assertThat(queryWithSchema.getKeyspaceHash()).isEqualTo(hash);
                assertThat(queryWithSchema.getQuery().getCql())
                    .isEqualTo("SELECT * FROM %s".formatted(table));
              })
          .anySatisfy(
              queryWithSchema -> {
                assertThat(queryWithSchema.getKeyspaceName()).isEqualTo(keyspace);
                assertThat(queryWithSchema.getKeyspaceHash()).isEqualTo(updatedHash);
                assertThat(queryWithSchema.getQuery().getCql())
                    .isEqualTo("SELECT column FROM %s".formatted(table));
              });
    }

    @Test
    public void keyspaceDoesNotExist() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      RuntimeException exception = new RuntimeException("my-exception");
      Function<String, Uni<? extends QueryOuterClass.Response>> missingKeyspace =
          k -> Uni.createFrom().failure(exception);

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

      UniAssertSubscriber<QueryOuterClass.Response> result =
          service
              .executeOptimistic(keyspace, table, missingKeyspace, (t) -> null)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      // assert
      Throwable failure = result.awaitFailure().getFailure();
      assertThat(failure).isEqualTo(exception);

      // verify no changes in the keyspace name
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));
    }
  }

  @Nested
  class ExecuteAuthorizedOptimistic {

    @Test
    public void happyPath() {
      // first invoke schema manager to be cached
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      int hash = RandomUtils.nextInt();
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable = Schema.CqlTable.newBuilder().setName(table).build();
      Schema.CqlKeyspaceDescribe keyspaceResponse =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .setHash(Int32Value.of(hash))
              .addTables(cqlTable)
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspaceResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> cache =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      cache.awaitItem().assertCompleted();

      // then call the optimistic query service
      // this would first authorize the schema access
      QueryOuterClass.Response queryResponse =
          QueryOuterClass.Response.newBuilder().addWarnings("whatever").build();
      Schema.QueryWithSchemaResponse queryWithSchemaResponse =
          Schema.QueryWithSchemaResponse.newBuilder().setResponse(queryResponse).build();

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
                StreamObserver<Schema.QueryWithSchemaResponse> observer =
                    invocationOnMock.getArgument(1);

                observer.onNext(queryWithSchemaResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQueryWithSchema(any(), any());

      // mapping function
      Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryFunction =
          t -> {
            String cql = "SELECT FROM %s".formatted(t.getName());
            QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
            return Uni.createFrom().item(query);
          };

      UniAssertSubscriber<QueryOuterClass.Response> result =
          service
              .executeAuthorizedOptimistic(
                  keyspace, table, k -> Uni.createFrom().nothing(), queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(queryResponse).assertCompleted();

      // verify bridge calls
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verify(bridgeService).executeQueryWithSchema(queryWithSchemaCaptor.capture(), any());
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));
      assertThat(schemaReadsCaptor.getAllValues())
          .singleElement()
          .extracting(Schema.AuthorizeSchemaReadsRequest::getSchemaReadsList)
          .satisfies(
              reads ->
                  assertThat(reads)
                      .singleElement()
                      .satisfies(
                          read -> {
                            assertThat(read.getKeyspaceName()).isEqualTo(keyspace);
                            assertThat(read.getElementType())
                                .isEqualTo(Schema.SchemaRead.ElementType.KEYSPACE);
                          }));
      assertThat(queryWithSchemaCaptor.getAllValues())
          .allSatisfy(
              queryWithSchema -> {
                assertThat(queryWithSchema.getKeyspaceName()).isEqualTo(keyspace);
                assertThat(queryWithSchema.getKeyspaceHash()).isEqualTo(hash);
                assertThat(queryWithSchema.getQuery().getCql())
                    .isEqualTo("SELECT FROM %s".formatted(table));
              });
    }
  }
}
