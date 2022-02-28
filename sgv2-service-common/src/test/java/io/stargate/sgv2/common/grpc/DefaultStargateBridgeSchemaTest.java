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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.common.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import io.stargate.proto.QueryOuterClass.SchemaChange.Target;
import io.stargate.proto.Schema.AuthorizeSchemaReadsRequest;
import io.stargate.proto.Schema.AuthorizeSchemaReadsResponse;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.DescribeKeyspaceQuery;
import io.stargate.proto.Schema.DescribeTableQuery;
import io.stargate.proto.Schema.GetSchemaNotificationsParams;
import io.stargate.proto.Schema.SchemaNotification;
import io.stargate.proto.StargateBridgeGrpc.StargateBridgeImplBase;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultStargateBridgeSchemaTest {

  private static final String SERVER_NAME = "MockBridge";
  private static final String AUTH_TOKEN = "MockAuthToken";
  private static final CqlKeyspaceDescribe NOT_FOUND = CqlKeyspaceDescribe.newBuilder().build();

  private Server server;
  private ManagedChannel channel;
  private MockBridgeService bridge;
  private ScheduledExecutorService executor;

  private DefaultStargateBridgeSchema schema;

  @BeforeEach
  public void setup() throws IOException {
    bridge = spy(new MockBridgeService());
    server =
        InProcessServerBuilder.forName(SERVER_NAME).directExecutor().addService(bridge).build();
    server.start();
    channel = InProcessChannelBuilder.forName(SERVER_NAME).usePlaintext().build();
    executor = Executors.newScheduledThreadPool(1);
    schema = new DefaultStargateBridgeSchema(channel, AUTH_TOKEN, executor);
  }

  @AfterEach
  public void teardown() {
    server.shutdownNow();
    executor.shutdownNow();
  }

  @Test
  public void shouldPopulateCacheOnFirstCall() {
    // Given
    CqlKeyspaceDescribe bridgeKs = newKeyspace("ks", "1");
    bridge.mockNextDescribe(bridgeKs);

    // When
    CqlKeyspaceDescribe clientKs = schema.getKeyspace("ks");

    // Then
    assertThat(clientKs).isEqualTo(bridgeKs);
    verify(bridge).describeKeyspace(any(), any());
  }

  @Test
  public void shouldNotCallBridgeOnceCached() {
    // Given
    CqlKeyspaceDescribe bridgeKs = newKeyspace("ks", "1");
    bridge.mockNextDescribe(bridgeKs);
    schema.getKeyspace("ks");

    // When
    CqlKeyspaceDescribe clientKs = schema.getKeyspace("ks");

    // Then
    assertThat(clientKs).isEqualTo(bridgeKs);
    verify(bridge, times(1)).describeKeyspace(any(), any());
  }

  @Test
  public void shouldInvalidateWhenBridgeSendsNotification() throws InterruptedException {
    // Given
    KeyspaceInvalidationListener listener = mock(KeyspaceInvalidationListener.class);
    schema.register(listener);

    CqlKeyspaceDescribe bridgeKs1 = newKeyspace("ks", "1");
    bridge.mockNextDescribe(bridgeKs1);
    schema.getKeyspace("ks");

    // When
    CqlKeyspaceDescribe bridgeKs2 = newKeyspace("ks", "2");
    bridge.mockNextDescribe(bridgeKs2);
    bridge.sendMockNotification(SchemaChange.Type.UPDATED, "ks");

    // Then
    await().untilAsserted(() -> assertThat(schema.getKeyspace("ks")).isEqualTo(bridgeKs2));
    verify(bridge, times(2)).describeKeyspace(any(), any());
    verify(listener).onKeyspaceInvalidated("ks");
  }

  @Test
  public void shouldHandleDeletion() {
    // Given
    KeyspaceInvalidationListener listener = mock(KeyspaceInvalidationListener.class);
    schema.register(listener);

    CqlKeyspaceDescribe bridgeKs1 = newKeyspace("ks", "1");
    bridge.mockNextDescribe(bridgeKs1);
    schema.getKeyspace("ks");

    // When
    bridge.mockNextDescribe(NOT_FOUND);
    bridge.sendMockNotification(SchemaChange.Type.DROPPED, "ks");

    // Then
    await().untilAsserted(() -> assertThat(schema.getKeyspace("ks")).isNull());
    verify(bridge, times(2)).describeKeyspace(any(), any());
    verify(listener).onKeyspaceInvalidated("ks");
  }

  private CqlKeyspaceDescribe newKeyspace(String name, String version) {
    return CqlKeyspaceDescribe.newBuilder()
        .setCqlKeyspace(
            CqlKeyspace.newBuilder()
                .setName(name)
                // dummy option to simulate different versions of the same keyspace
                .putOptions("version", version))
        .build();
  }

  static class MockBridgeService extends StargateBridgeImplBase {

    private AtomicReference<StreamObserver<SchemaNotification>> notifications =
        new AtomicReference<>();
    private Queue<CqlKeyspaceDescribe> pendingDescribes = new ConcurrentLinkedQueue<>();

    void sendMockNotification(SchemaChange.Type type, String keyspaceName) {
      notifications
          .get()
          .onNext(
              SchemaNotification.newBuilder()
                  .setChange(
                      SchemaChange.newBuilder()
                          .setChangeType(type)
                          .setTarget(Target.KEYSPACE)
                          .setKeyspace(keyspaceName))
                  .build());
    }

    void mockNextDescribe(CqlKeyspaceDescribe describe) {
      assertThat(pendingDescribes.offer(describe)).isTrue();
    }

    @Override
    public void describeKeyspace(
        DescribeKeyspaceQuery request, StreamObserver<CqlKeyspaceDescribe> responseObserver) {
      CqlKeyspaceDescribe describe = pendingDescribes.poll();
      assertThat(describe)
          .as("The test should have called mockNextDescribe() to mock this result")
          .isNotNull();
      if (describe == NOT_FOUND) {
        responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
      } else {
        responseObserver.onNext(describe);
        responseObserver.onCompleted();
      }
    }

    @Override
    public void getSchemaNotifications(
        GetSchemaNotificationsParams request, StreamObserver<SchemaNotification> responseObserver) {
      if (this.notifications.compareAndSet(null, responseObserver)) {
        responseObserver.onNext(
            SchemaNotification.newBuilder()
                .setReady(SchemaNotification.Ready.newBuilder())
                .build());
      } else {
        fail("This test expects at most one getSchemaNotifications call");
      }
    }

    @Override
    public void executeQuery(Query request, StreamObserver<Response> responseObserver) {
      fail("Should not be called in this test");
    }

    @Override
    public void executeBatch(Batch request, StreamObserver<Response> responseObserver) {
      fail("Should not be called in this test");
    }

    @Override
    public void describeTable(
        DescribeTableQuery request, StreamObserver<CqlTable> responseObserver) {
      fail("Should not be called in this test");
    }

    @Override
    public void authorizeSchemaReads(
        AuthorizeSchemaReadsRequest request,
        StreamObserver<AuthorizeSchemaReadsResponse> responseObserver) {
      fail("Should not be called in this test");
    }
  }
}
