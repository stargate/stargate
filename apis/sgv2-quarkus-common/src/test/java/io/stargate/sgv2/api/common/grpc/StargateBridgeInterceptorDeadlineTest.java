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
import static org.mockito.Mockito.doAnswer;

import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.MutinyStargateBridgeGrpc;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.testprofiles.FixedTenantTestProfile;
import io.stargate.sgv2.common.bridge.BridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(StargateBridgeInterceptorDeadlineTest.Profile.class)
@Disabled("Disabled due to intermmittent failure, needs analysis")
class StargateBridgeInterceptorDeadlineTest extends BridgeTest {

  public static class Profile extends FixedTenantTestProfile
      implements NoGlobalResourcesTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("stargate.grpc.call-deadline", "PT1S")
          .build();
    }
  }

  @GrpcClient("bridge")
  MutinyStargateBridgeGrpc.MutinyStargateBridgeStub client;

  ArgumentCaptor<Metadata> headersCaptor;

  @BeforeEach
  public void init() {
    headersCaptor = ArgumentCaptor.forClass(Metadata.class);
  }

  @Test
  public void happyPath() {
    // call bridge without http call
    String keyspaceName = RandomStringUtils.randomAlphanumeric(16);
    Schema.DescribeKeyspaceQuery query =
        Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build();
    Schema.CqlKeyspaceDescribe response =
        Schema.CqlKeyspaceDescribe.newBuilder()
            .setCqlKeyspace(Schema.CqlKeyspace.newBuilder().setName(keyspaceName).build())
            .buildPartial();

    // simulate deadline exceeded on the bridge using semaphore
    Semaphore semaphore = new Semaphore(0);
    doAnswer(
            invocationOnMock -> {
              semaphore.acquire();
              return null;
            })
        .when(bridgeService)
        .describeKeyspace(any(), any());

    UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
        client.describeKeyspace(query).subscribe().withSubscriber(UniAssertSubscriber.create());

    try {
      // verify result
      Throwable failure = result.awaitFailure().getFailure();
      assertThat(failure)
          .isInstanceOfSatisfying(
              StatusRuntimeException.class,
              e -> {
                assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
              });
    } finally {
      // release the thread that blocks on semaphore
      semaphore.release();
    }
  }
}
