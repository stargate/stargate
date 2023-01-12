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

package io.stargate.sgv2.api.common.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.Int32Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CaffeineCache;
import io.quarkus.cache.CompositeCacheKey;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.BridgeTest;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.grpc.UnauthorizedKeyspaceException;
import io.stargate.sgv2.api.common.grpc.UnauthorizedTableException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
class SchemaManagerTest extends BridgeTest {

  @Inject SchemaManager schemaManager;

  @Inject
  @CacheName("keyspace-cache")
  Cache keyspaceCache;

  @GrpcClient("bridge")
  StargateBridge bridge;

  @InjectMock StargateRequestInfo requestInfo;

  ArgumentCaptor<Schema.DescribeKeyspaceQuery> describeKeyspaceCaptor;

  ArgumentCaptor<Schema.AuthorizeSchemaReadsRequest> schemaReadsCaptor;

  ArgumentCaptor<QueryOuterClass.Query> queryCaptor;

  ArgumentCaptor<Schema.QueryWithSchema> queryWithSchemaCaptor;

  @BeforeEach
  public void init() {
    describeKeyspaceCaptor = ArgumentCaptor.forClass(Schema.DescribeKeyspaceQuery.class);
    schemaReadsCaptor = ArgumentCaptor.forClass(Schema.AuthorizeSchemaReadsRequest.class);
    queryCaptor = ArgumentCaptor.forClass(QueryOuterClass.Query.class);
    queryWithSchemaCaptor = ArgumentCaptor.forClass(Schema.QueryWithSchema.class);
    doAnswer(invocation -> bridge).when(requestInfo).getStargateBridge();
  }

  @Nested
  class GetKeyspace {

    @Test
    public void doesNotExist() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);

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

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(null).assertCompleted();
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace not in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .doesNotContain(new CompositeCacheKey(keyspace, Optional.empty()));
    }

    @Test
    public void errorThenCached() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      Status status = Status.UNAVAILABLE;
      StatusRuntimeException error = new StatusRuntimeException(status);

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onError(error);
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      result.awaitFailure().assertFailedWith(StatusRuntimeException.class);

      Schema.CqlKeyspace value = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(value).build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> updatedResult =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      updatedResult.awaitItem().assertItem(response).assertCompleted();

      // 2 calls, first error so ignore, second fetched no need to revalidate as fresh
      verify(bridgeService, times(2)).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));
    }

    @Test
    public void notCached() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace value = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(value).build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(response).assertCompleted();
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));
    }

    @Test
    public void notCachedMultiTenancy() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String tenant1 = RandomStringUtils.randomAlphanumeric(16);
      String tenant2 = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace value = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(value).build();

      when(requestInfo.getTenantId()).thenReturn(Optional.of(tenant1));
      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      // this is unfortunate, the first time it's cached it does two calls to the bridge
      result.awaitItem().assertItem(response).assertCompleted();
      verify(bridgeService, times(1)).describeKeyspace(describeKeyspaceCaptor.capture(), any());

      when(requestInfo.getTenantId()).thenReturn(Optional.of(tenant2));
      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result2 =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      // this is unfortunate, the first time it's cached it does two calls to the bridge (2x
      // tenants)
      result2.awaitItem().assertItem(response).assertCompleted();
      verify(bridgeService, times(2)).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.of(tenant1)))
          .contains(new CompositeCacheKey(keyspace, Optional.of(tenant2)));
    }

    @Test
    public void cachedUpdated() throws Exception {
      int hash = RandomUtils.nextInt();
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace value = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(value)
              .setHash(Int32Value.newBuilder().setValue(hash))
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      result.awaitItem().assertItem(response).assertCompleted();

      int updatedHash = RandomUtils.nextInt();
      Schema.CqlKeyspaceDescribe updatedResponse =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(value)
              .setHash(Int32Value.newBuilder().setValue(updatedHash))
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(updatedResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> updatedResult =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      updatedResult.awaitItem().assertItem(updatedResponse).assertCompleted();

      verify(bridgeService, times(2)).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));

      // assert keyspace in cache
      CompletableFuture<Object> cachedKeyspace =
          keyspaceCache
              .as(CaffeineCache.class)
              .getIfPresent(new CompositeCacheKey(keyspace, Optional.empty()));
      assertThat(cachedKeyspace).isNotNull();
      assertThat(cachedKeyspace.get()).isEqualTo(updatedResponse);
    }

    @Test
    public void cachedNotUpdated() {
      int hash = RandomUtils.nextInt();
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace value = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(value)
              .setHash(Int32Value.newBuilder().setValue(hash))
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      result.awaitItem().assertItem(response).assertCompleted();

      Schema.CqlKeyspaceDescribe updatedResponse =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setHash(Int32Value.newBuilder().setValue(hash))
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(updatedResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> updatedResult =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      updatedResult.awaitItem().assertItem(response).assertCompleted();

      verify(bridgeService, times(2)).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));
    }

    @Test
    public void cachedThenRemoved() {
      int hash = RandomUtils.nextInt();
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace value = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(value)
              .setHash(Int32Value.newBuilder().setValue(hash))
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      result.awaitItem().assertItem(response).assertCompleted();

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

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> updatedResult =
          schemaManager
              .getKeyspace(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      updatedResult.awaitItem().assertItem(null).assertCompleted();

      verify(bridgeService, times(2)).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));

      // assert keyspace not in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .doesNotContain(new CompositeCacheKey(keyspace, Optional.empty()));
    }

    @Test
    public void cachedHashNotValidated() {
      int hash = RandomUtils.nextInt();
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace value = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(value)
              .setHash(Int32Value.newBuilder().setValue(hash))
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspace(keyspace, false)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      result.awaitItem().assertItem(response).assertCompleted();

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> cachedResult =
          schemaManager
              .getKeyspace(keyspace, false)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      cachedResult.awaitItem().assertItem(response).assertCompleted();

      verify(bridgeService, times(1)).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));
    }
  }

  @Nested
  class GetKeyspaceAuthorized {

    @Test
    public void happyPath() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace value = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(true).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(value).build();

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
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspaceAuthorized(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(response).assertCompleted();
      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));

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
                            assertThat(read.getKeyspaceName()).isEqualTo(keyspace);
                            assertThat(read.getElementType())
                                .isEqualTo(Schema.SchemaRead.ElementType.KEYSPACE);
                          }));
    }

    @Test
    public void cachedHashNotValidated() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace value = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(true).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(value).build();

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
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspaceAuthorized(keyspace, false)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      result.awaitItem().assertItem(response).assertCompleted();

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> cachedResult =
          schemaManager
              .getKeyspaceAuthorized(keyspace, false)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());
      cachedResult.awaitItem().assertItem(response).assertCompleted();

      verify(bridgeService, times(2)).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));

      // asert auth request
      assertThat(schemaReadsCaptor.getAllValues())
          .hasSize(2)
          .allSatisfy(
              captured ->
                  assertThat(captured)
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
                                      })));
    }

    @Test
    public void notAuthorized() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace value = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(false).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(value).build();

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
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspaceAuthorized(keyspace)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitFailure().assertFailedWith(UnauthorizedKeyspaceException.class);
      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace not in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .doesNotContain(new CompositeCacheKey(keyspace, Optional.empty()));
    }
  }

  @Nested
  class GetKeyspaces {

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
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(queryResponse);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      List<Schema.CqlKeyspaceDescribe> result =
          schemaManager
              .getKeyspaces()
              .subscribe()
              .withSubscriber(AssertSubscriber.create())
              .awaitNextItems(2)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      verify(bridgeService, times(2)).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).executeQuery(queryCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert both keyspaces in cache
      Set<Object> cacheState = keyspaceCache.as(CaffeineCache.class).keySet();
      assertThat(cacheState).contains(new CompositeCacheKey(keyspace1, Optional.empty()));
      assertThat(cacheState).contains(new CompositeCacheKey(keyspace2, Optional.empty()));

      // assert result
      assertThat(result)
          .hasSize(2)
          .extracting(Schema.CqlKeyspaceDescribe::getCqlKeyspace)
          .flatExtracting(Schema.CqlKeyspace::getName)
          .contains(keyspace1, keyspace2);

      // assert queries
      assertThat(describeKeyspaceCaptor.getAllValues())
          .hasSize(2)
          .flatExtracting(Schema.DescribeKeyspaceQuery::getKeyspaceName)
          .contains(keyspace1, keyspace2);
      assertThat(queryCaptor.getAllValues())
          .singleElement()
          .satisfies(
              query ->
                  assertThat(query.getCql())
                      .isEqualTo("SELECT keyspace_name FROM system_schema.keyspaces"));
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

      schemaManager
          .getKeyspaces()
          .subscribe()
          .withSubscriber(AssertSubscriber.create())
          .awaitCompletion()
          .assertCompleted()
          .assertHasNotReceivedAnyItem();

      verify(bridgeService).executeQuery(queryCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert queries
      assertThat(queryCaptor.getAllValues())
          .singleElement()
          .satisfies(
              query ->
                  assertThat(query.getCql())
                      .isEqualTo("SELECT keyspace_name FROM system_schema.keyspaces"));
    }

    @Test
    public void deletedBetweenCalls() {
      String keyspace1 = RandomStringUtils.randomAlphanumeric(16);
      String keyspace2 = RandomStringUtils.randomAlphanumeric(16);
      QueryOuterClass.ResultSet.Builder resultSet =
          QueryOuterClass.ResultSet.newBuilder()
              .addRows(QueryOuterClass.Row.newBuilder().addValues(Values.of(keyspace1)).build())
              .addRows(QueryOuterClass.Row.newBuilder().addValues(Values.of(keyspace2)).build());
      QueryOuterClass.Response queryResponse =
          QueryOuterClass.Response.newBuilder().setResultSet(resultSet).build();

      doAnswer(
              invocationOnMock -> {
                Schema.DescribeKeyspaceQuery query = invocationOnMock.getArgument(0);
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                String keyspace = query.getKeyspaceName();
                if (Objects.equals(keyspace, keyspace1)) {
                  Schema.CqlKeyspace value =
                      Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
                  Schema.CqlKeyspaceDescribe cqlKeyspace =
                      Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(value).build();
                  observer.onNext(cqlKeyspace);
                  observer.onCompleted();
                } else {
                  Status status = Status.NOT_FOUND;
                  observer.onError(new StatusRuntimeException(status));
                }
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());
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
          schemaManager
              .getKeyspaces()
              .subscribe()
              .withSubscriber(AssertSubscriber.create())
              .awaitNextItems(1)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      verify(bridgeService, times(2)).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).executeQuery(queryCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert only fetched keyspaces in cache
      Set<Object> cacheState = keyspaceCache.as(CaffeineCache.class).keySet();
      assertThat(cacheState).contains(new CompositeCacheKey(keyspace1, Optional.empty()));
      assertThat(cacheState).doesNotContain(new CompositeCacheKey(keyspace2, Optional.empty()));

      // assert result
      assertThat(result)
          .singleElement()
          .satisfies(k -> assertThat(k.getCqlKeyspace().getName()).isEqualTo(keyspace1));

      // assert queries
      assertThat(describeKeyspaceCaptor.getAllValues())
          .hasSize(2)
          .flatExtracting(Schema.DescribeKeyspaceQuery::getKeyspaceName)
          .contains(keyspace1, keyspace2);
      assertThat(queryCaptor.getAllValues())
          .singleElement()
          .satisfies(
              query ->
                  assertThat(query.getCql())
                      .isEqualTo("SELECT keyspace_name FROM system_schema.keyspaces"));
    }
  }

  @Nested
  class GetKeyspacesAuthorized {

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
          schemaManager
              .getKeyspacesAuthorized()
              .subscribe()
              .withSubscriber(AssertSubscriber.create())
              .awaitNextItems(1)
              .awaitCompletion()
              .assertCompleted()
              .getItems();

      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).executeQuery(queryCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // only authorized keyspace in cache
      Set<Object> cacheState = keyspaceCache.as(CaffeineCache.class).keySet();
      assertThat(cacheState).contains(new CompositeCacheKey(keyspace1, Optional.empty()));
      assertThat(cacheState).doesNotContain(new CompositeCacheKey(keyspace2, Optional.empty()));

      // assert result
      assertThat(result)
          .singleElement()
          .satisfies(k -> assertThat(k.getCqlKeyspace().getName()).isEqualTo(keyspace1));

      // assert queries
      assertThat(describeKeyspaceCaptor.getAllValues())
          .singleElement()
          .satisfies(k -> assertThat(k.getKeyspaceName()).isEqualTo(keyspace1));

      assertThat(queryCaptor.getAllValues())
          .singleElement()
          .satisfies(
              query ->
                  assertThat(query.getCql())
                      .isEqualTo("SELECT keyspace_name FROM system_schema.keyspaces"));

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
                            assertThat(read.getKeyspaceName()).isEqualTo(keyspace1);
                            assertThat(read.getElementType())
                                .isEqualTo(Schema.SchemaRead.ElementType.KEYSPACE);
                          })
                      .anySatisfy(
                          read -> {
                            assertThat(read.getKeyspaceName()).isEqualTo(keyspace2);
                            assertThat(read.getElementType())
                                .isEqualTo(Schema.SchemaRead.ElementType.KEYSPACE);
                          }));
    }
  }

  @Nested
  class GetTable {

    @Test
    public void happyPath() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable = Schema.CqlTable.newBuilder().setName(table).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .addTables(cqlTable)
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlTable> result =
          schemaManager
              .getTable(
                  keyspace,
                  table,
                  (k) -> {
                    throw new RuntimeException("Must not throw!");
                  })
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(cqlTable).assertCompleted();
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));
    }

    @Test
    public void missingTable() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable = Schema.CqlTable.newBuilder().setName("other").build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .addTables(cqlTable)
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlTable> result =
          schemaManager
              .getTable(keyspace, table, (k) -> Uni.createFrom().nothing())
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(null).assertCompleted();
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace still in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));
    }

    @Test
    public void missingKeyspace() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);

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

      Throwable exception = new RuntimeException("Missing keyspace test exception.");
      UniAssertSubscriber<Schema.CqlTable> result =
          schemaManager
              .getTable(keyspace, table, (k) -> Uni.createFrom().failure(exception))
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      Throwable failure = result.awaitFailure().getFailure();
      assertThat(failure).isEqualTo(exception);
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace not in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .doesNotContain(new CompositeCacheKey(keyspace, Optional.empty()));
    }
  }

  @Nested
  class GetTables {

    @Test
    public void happyPath() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table1 = RandomStringUtils.randomAlphanumeric(16);
      String table2 = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable1 = Schema.CqlTable.newBuilder().setName(table1).build();
      Schema.CqlTable cqlTable2 = Schema.CqlTable.newBuilder().setName(table2).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .addTables(cqlTable1)
              .addTables(cqlTable2)
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      schemaManager
          .getTables(
              keyspace,
              (k) -> {
                throw new RuntimeException("Must not throw!");
              })
          .subscribe()
          .withSubscriber(AssertSubscriber.create())
          .awaitNextItems(2)
          .assertItems(cqlTable1, cqlTable2)
          .awaitCompletion()
          .assertCompleted();

      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));
    }

    @Test
    public void noTables() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(cqlKeyspace).build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      schemaManager
          .getTables(keyspace, (k) -> Uni.createFrom().nothing())
          .subscribe()
          .withSubscriber(AssertSubscriber.create())
          .awaitCompletion()
          .assertHasNotReceivedAnyItem()
          .assertCompleted();

      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace still in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));
    }

    @Test
    public void missingKeyspace() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);

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

      Throwable exception = new RuntimeException("Missing keyspace test exception.");
      Throwable failure =
          schemaManager
              .getTables(keyspace, (k) -> Uni.createFrom().failure(exception))
              .subscribe()
              .withSubscriber(AssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(failure).isEqualTo(exception);
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace not in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .doesNotContain(new CompositeCacheKey(keyspace, Optional.empty()));
    }
  }

  @Nested
  class GetTableAuthorized {

    @Test
    public void happyPath() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(true).build();
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable = Schema.CqlTable.newBuilder().setName(table).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .addTables(cqlTable)
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
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlTable> result =
          schemaManager
              .getTableAuthorized(keyspace, table, (k) -> Uni.createFrom().nothing())
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(cqlTable).assertCompleted();
      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));

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
                            assertThat(read.getKeyspaceName()).isEqualTo(keyspace);
                            assertThat(read.getElementName().getValue()).isEqualTo(table);
                            assertThat(read.getElementType())
                                .isEqualTo(Schema.SchemaRead.ElementType.TABLE);
                          }));
    }

    @Test
    public void notAuthorized() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder().addAuthorized(false).build();
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable = Schema.CqlTable.newBuilder().setName(table).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .addTables(cqlTable)
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
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<Schema.CqlTable> result =
          schemaManager
              .getTableAuthorized(keyspace, table, (k) -> Uni.createFrom().nothing())
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitFailure().assertFailedWith(UnauthorizedTableException.class);
      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
    }
  }

  @Nested
  class GetTablesAuthorized {

    @Test
    public void happyPath() {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table1 = RandomStringUtils.randomAlphanumeric(16);
      String table2 = RandomStringUtils.randomAlphanumeric(16);
      Schema.AuthorizeSchemaReadsResponse authResponse =
          Schema.AuthorizeSchemaReadsResponse.newBuilder()
              .addAllAuthorized(Arrays.asList(false, true))
              .build();
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable1 = Schema.CqlTable.newBuilder().setName(table1).build();
      Schema.CqlTable cqlTable2 = Schema.CqlTable.newBuilder().setName(table2).build();
      Schema.CqlKeyspaceDescribe response =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .addTables(cqlTable1)
              .addTables(cqlTable2)
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
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      schemaManager
          .getTablesAuthorized(
              keyspace,
              (k) -> {
                throw new RuntimeException("Must not throw!");
              })
          .subscribe()
          .withSubscriber(AssertSubscriber.create())
          .awaitNextItem()
          .assertItems(cqlTable2)
          .awaitCompletion()
          .assertCompleted();

      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet())
          .contains(new CompositeCacheKey(keyspace, Optional.empty()));

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
                            assertThat(read.getKeyspaceName()).isEqualTo(keyspace);
                            assertThat(read.getElementName().getValue()).isEqualTo(table1);
                            assertThat(read.getElementType())
                                .isEqualTo(Schema.SchemaRead.ElementType.TABLE);
                          })
                      .anySatisfy(
                          read -> {
                            assertThat(read.getKeyspaceName()).isEqualTo(keyspace);
                            assertThat(read.getElementName().getValue()).isEqualTo(table2);
                            assertThat(read.getElementType())
                                .isEqualTo(Schema.SchemaRead.ElementType.TABLE);
                          }));
    }
  }

  @Nested
  class QueryWithSchema {

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
          schemaManager
              .queryWithSchema(
                  keyspace,
                  table,
                  (k) -> {
                    throw new RuntimeException("Must not throw!");
                  },
                  queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(queryResponse).assertCompleted();

      // verify bridge calls
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).executeQueryWithSchema(queryWithSchemaCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
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
    public void happyPathWithoutPreCaching() {
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
          schemaManager
              .queryWithSchema(
                  keyspace,
                  table,
                  (k) -> {
                    throw new RuntimeException("Must not throw!");
                  },
                  queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(queryResponse).assertCompleted();

      // verify bridge calls
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).executeQueryWithSchema(queryWithSchemaCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
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
          schemaManager
              .queryWithSchema(keyspace, table, missingKeyspace, queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      Throwable failure = result.awaitFailure().getFailure();
      assertThat(failure).isEqualTo(exception);

      // verify bridge calls
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).executeQueryWithSchema(queryWithSchemaCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
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
    public void keyspaceUpdated() throws Exception {
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
          schemaManager
              .queryWithSchema(keyspace, table, k -> Uni.createFrom().nothing(), queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(queryResponse).assertCompleted();

      // assert updated keyspace in hash
      // assert keyspace in cache
      CompletableFuture<Object> cachedKeyspace =
          keyspaceCache
              .as(CaffeineCache.class)
              .getIfPresent(new CompositeCacheKey(keyspace, Optional.empty()));
      assertThat(cachedKeyspace).isNotNull();
      assertThat(cachedKeyspace.get()).isEqualTo(updatedKeyspace);

      // verify bridge calls
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService, times(2))
          .executeQueryWithSchema(queryWithSchemaCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
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
    public void tableDoesNotExistRevalidate() throws Exception {
      // first invoke schema manager to be cached
      // but ensure cached version has no table
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      int withTableHash = RandomUtils.nextInt();
      int withoutTableHash = RandomUtils.nextInt();
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlTable cqlTable = Schema.CqlTable.newBuilder().setName(table).build();
      Schema.CqlKeyspaceDescribe keyspaceResponse =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .setHash(Int32Value.of(withTableHash))
              .addTables(cqlTable)
              .build();
      Schema.CqlKeyspaceDescribe keyspaceWithoutTableResponse =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .setHash(Int32Value.of(withoutTableHash))
              .build();

      AtomicInteger describeCalls = new AtomicInteger(0);
      doAnswer(
              invocationOnMock -> {
                int call = describeCalls.getAndIncrement();
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(call == 0 ? keyspaceWithoutTableResponse : keyspaceResponse);
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
          schemaManager
              .queryWithSchema(
                  keyspace,
                  table,
                  (k) -> {
                    throw new RuntimeException("Must not throw!");
                  },
                  queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(queryResponse).assertCompleted();

      // assert updated keyspace in hash
      // assert keyspace in cache
      CompletableFuture<Object> cachedKeyspace =
          keyspaceCache
              .as(CaffeineCache.class)
              .getIfPresent(new CompositeCacheKey(keyspace, Optional.empty()));
      assertThat(cachedKeyspace).isNotNull();
      assertThat(cachedKeyspace.get()).isEqualTo(keyspaceResponse);

      // verify bridge calls
      verify(bridgeService, times(2)).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).executeQueryWithSchema(queryWithSchemaCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));
      assertThat(queryWithSchemaCaptor.getAllValues())
          .allSatisfy(
              queryWithSchema -> {
                assertThat(queryWithSchema.getKeyspaceName()).isEqualTo(keyspace);
                assertThat(queryWithSchema.getKeyspaceHash()).isEqualTo(withTableHash);
                assertThat(queryWithSchema.getQuery().getCql())
                    .isEqualTo("SELECT FROM %s".formatted(table));
              });
    }

    @Test
    public void tableDoesNotExist() {
      // first invoke schema manager to be cached
      // but ensure cached version has no table
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String table = RandomStringUtils.randomAlphanumeric(16);
      int withoutTableHash = RandomUtils.nextInt();
      Schema.CqlKeyspace cqlKeyspace = Schema.CqlKeyspace.newBuilder().setName(keyspace).build();
      Schema.CqlKeyspaceDescribe keyspaceWithoutTableResponse =
          Schema.CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(cqlKeyspace)
              .setHash(Int32Value.of(withoutTableHash))
              .build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspaceWithoutTableResponse);
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
      RuntimeException notExistingEx = new RuntimeException("Table not existing");
      Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryFunction =
          t -> {
            if (t == null) {
              throw notExistingEx;
            }
            String cql = "SELECT FROM %s".formatted(t.getName());
            QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
            return Uni.createFrom().item(query);
          };

      Throwable failure =
          schemaManager
              .queryWithSchema(
                  keyspace,
                  table,
                  (k) -> {
                    throw new RuntimeException("Must not throw!");
                  },
                  queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(failure).isEqualTo(notExistingEx);

      // verify bridge calls
      verify(bridgeService, times(2)).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));
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
          schemaManager
              .queryWithSchema(keyspace, table, missingKeyspace, (t) -> null)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      // assert
      Throwable failure = result.awaitFailure().getFailure();
      assertThat(failure).isEqualTo(exception);

      // verify no changes in the keyspace name
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
      assertThat(describeKeyspaceCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));
    }
  }

  @Nested
  class QueryWithSchemaAuthorized {

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
          schemaManager
              .queryWithSchemaAuthorized(
                  keyspace, table, k -> Uni.createFrom().nothing(), queryFunction)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(queryResponse).assertCompleted();

      // verify bridge calls
      verify(bridgeService).describeKeyspace(describeKeyspaceCaptor.capture(), any());
      verify(bridgeService).authorizeSchemaReads(schemaReadsCaptor.capture(), any());
      verify(bridgeService).executeQueryWithSchema(queryWithSchemaCaptor.capture(), any());
      verifyNoMoreInteractions(bridgeService);
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
