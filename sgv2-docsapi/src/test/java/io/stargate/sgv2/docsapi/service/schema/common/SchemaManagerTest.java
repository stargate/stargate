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

package io.stargate.sgv2.docsapi.service.schema.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.protobuf.Int32Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CaffeineCache;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.proto.Schema;
import io.stargate.sgv2.docsapi.BridgeTest;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
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

  @InjectMock StargateRequestInfo requestInfo;

  ArgumentCaptor<Schema.DescribeKeyspaceQuery> queryCaptor;

  @BeforeEach
  public void initCaptor() {
    queryCaptor = ArgumentCaptor.forClass(Schema.DescribeKeyspaceQuery.class);
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
      verify(bridgeService).describeKeyspace(queryCaptor.capture(), any());

      // assert keyspace not in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet()).doesNotContain(keyspace);
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

      verify(bridgeService, times(3)).describeKeyspace(queryCaptor.capture(), any());
      assertThat(queryCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet()).contains(keyspace);
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

      // this is unfortunate, the first time it's cached it does two calls to the bridge
      result.awaitItem().assertItem(response).assertCompleted();
      verify(bridgeService, times(2)).describeKeyspace(queryCaptor.capture(), any());

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet()).contains(keyspace);
    }

    @Test
    public void cachedUpdated() {
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

      verify(bridgeService, times(3)).describeKeyspace(queryCaptor.capture(), any());
      assertThat(queryCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet()).contains(keyspace);
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

      verify(bridgeService, times(3)).describeKeyspace(queryCaptor.capture(), any());
      assertThat(queryCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));

      // assert keyspace in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet()).contains(keyspace);
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

      verify(bridgeService, times(3)).describeKeyspace(queryCaptor.capture(), any());
      assertThat(queryCaptor.getAllValues())
          .allSatisfy(r -> assertThat(r.getKeyspaceName()).isEqualTo(keyspace));

      // assert keyspace nto in cache
      assertThat(keyspaceCache.as(CaffeineCache.class).keySet()).doesNotContain(keyspace);
    }
  }
}
