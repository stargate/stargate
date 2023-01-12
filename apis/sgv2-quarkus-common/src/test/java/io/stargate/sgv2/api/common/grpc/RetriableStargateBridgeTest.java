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

package io.stargate.sgv2.api.common.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.BridgeTest;
import io.stargate.sgv2.api.common.grpc.qualifier.Retriable;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.util.Map;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
class RetriableStargateBridgeTest extends BridgeTest {

  @Retriable @Inject RetriableStargateBridge bridge;

  @Nested
  @TestProfile(RetriableStargateBridgeTest.Enabled.Profile.class)
  class Enabled {
    public static class Profile implements NoGlobalResourcesTestProfile {

      @Override
      public Map<String, String> getConfigOverrides() {
        return ImmutableMap.<String, String>builder()
            .put("stargate.grpc.retries.status-codes", "UNAVAILABLE,NOT_FOUND")
            .put("stargate.grpc.retries.max-attempts", "2")
            .build();
      }
    }

    @Test
    public void notRetriedOnResponse() {
      QueryOuterClass.Response response = QueryOuterClass.Response.newBuilder().build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
      bridge
          .executeQuery(request)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(response)
          .assertCompleted();

      // verify one call only
      verify(bridgeService).executeQuery(eq(request), any());
    }

    @Test
    public void notRetriedWrongStatusCode() {
      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                Status status = Status.UNIMPLEMENTED;
                observer.onError(new StatusRuntimeException(status));
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
      Throwable result =
          bridge
              .executeQuery(request)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(result)
          .isInstanceOfSatisfying(
              StatusRuntimeException.class,
              e -> assertThat(e.getStatus()).isEqualTo(Status.UNIMPLEMENTED));

      // verify one call only
      verify(bridgeService).executeQuery(eq(request), any());
    }

    @Test
    public void retried() {
      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                Status status = Status.UNAVAILABLE;
                observer.onError(new StatusRuntimeException(status));
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
      Throwable result =
          bridge
              .executeQuery(request)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(result)
          .isInstanceOfSatisfying(
              StatusRuntimeException.class,
              e -> assertThat(e.getStatus()).isEqualTo(Status.UNAVAILABLE));

      // verify 3 bridge calls, original + 2 retries
      // always same query
      verify(bridgeService, times(3)).executeQuery(eq(request), any());
    }

    @Test
    public void retriedAdditionalStatusCode() {
      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                Status status = Status.NOT_FOUND;
                observer.onError(new StatusRuntimeException(status));
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
      Throwable result =
          bridge
              .executeQuery(request)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(result)
          .isInstanceOfSatisfying(
              StatusRuntimeException.class,
              e -> assertThat(e.getStatus()).isEqualTo(Status.NOT_FOUND));

      // verify 3 bridge calls, original + 2 retries
      // always same query
      verify(bridgeService, times(3)).executeQuery(eq(request), any());
    }
  }

  @Nested
  @TestProfile(RetriableStargateBridgeTest.Disabled.Profile.class)
  class Disabled {

    public static class Profile implements NoGlobalResourcesTestProfile {

      @Override
      public Map<String, String> getConfigOverrides() {
        return ImmutableMap.<String, String>builder()
            .put("stargate.grpc.retries.enabled", "false")
            .put("stargate.grpc.retries.status-codes", "UNAVAILABLE,NOT_FOUND")
            .build();
      }
    }

    @Test
    public void disabledNoRetries() {
      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                Status status = Status.UNAVAILABLE;
                observer.onError(new StatusRuntimeException(status));
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
      Throwable result =
          bridge
              .executeQuery(request)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(result)
          .isInstanceOfSatisfying(
              StatusRuntimeException.class,
              e -> assertThat(e.getStatus()).isEqualTo(Status.UNAVAILABLE));

      // verify one class only
      verify(bridgeService).executeQuery(eq(request), any());
    }
  }
}
