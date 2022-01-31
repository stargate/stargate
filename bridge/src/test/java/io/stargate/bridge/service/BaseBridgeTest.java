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
package io.stargate.bridge.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.proto.StargateBridgeGrpc;
import io.stargate.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BaseBridgeTest {
  private static final String SERVER_NAME = "BridgeTests";

  private Server server;
  private ManagedChannel clientChannel;

  protected @Mock Persistence persistence;

  protected Connection connection = spy(mock(Connection.class));

  private ScheduledExecutorService executor;

  @AfterEach
  public void cleanUp() {
    try {
      if (server != null) {
        server.shutdown().awaitTermination();
      }
      if (executor != null) {
        executor.shutdownNow();
      }
      if (clientChannel != null) {
        clientChannel.shutdown().awaitTermination(60, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      throw new AssertionError("Unexpected interruption", e);
    }
  }

  protected StargateBridgeBlockingStub makeBlockingStub() {
    if (clientChannel == null) {
      clientChannel = InProcessChannelBuilder.forName(SERVER_NAME).usePlaintext().build();
    }
    return StargateBridgeGrpc.newBlockingStub(clientChannel);
  }

  protected void startServer(Persistence persistence) {
    startServer(new MockInterceptor(persistence));
  }

  protected void startServer(ServerInterceptor interceptor) {
    assertThat(server).isNull();
    executor = Executors.newScheduledThreadPool(1);
    server =
        InProcessServerBuilder.forName(SERVER_NAME)
            .directExecutor()
            .intercept(interceptor)
            .addService(new BridgeService(persistence, executor, 2))
            .build();
    try {
      server.start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
