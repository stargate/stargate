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
package io.stargate.grpc.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.stargate.db.BoundStatement;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Statement;
import io.stargate.db.schema.Column;
import io.stargate.grpc.codec.ValueCodecs;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.BatchQuery;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import io.stargate.proto.StargateGrpc;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BaseGrpcServiceTest {
  private static final String SERVER_NAME = "ServiceTests";

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

  protected StargateBlockingStub makeBlockingStub() {
    if (clientChannel == null) {
      clientChannel = InProcessChannelBuilder.forName(SERVER_NAME).usePlaintext().build();
    }
    return StargateGrpc.newBlockingStub(clientChannel);
  }

  protected StargateGrpc.StargateBlockingStub makeBlockingStubWithClientHeaders(
      Consumer<Metadata> addHeaders) {
    ManagedChannel originalChannel =
        InProcessChannelBuilder.forName(SERVER_NAME).usePlaintext().build();
    Channel channel =
        ClientInterceptors.intercept(originalChannel, new AddClientHeadersInterceptor(addHeaders));
    return StargateGrpc.newBlockingStub(channel);
  }

  protected QueryOuterClass.Response executeQuery(
      StargateBlockingStub stub, String cql, Value... values) {
    return stub.executeQuery(Query.newBuilder().setCql(cql).setValues(valuesOf(values)).build());
  }

  protected static Values.Builder valuesOf(Value... values) {
    return Values.newBuilder().addAllValues(Arrays.asList(values));
  }

  protected static QueryParameters.Builder cqlQueryParameters() {
    return QueryParameters.newBuilder();
  }

  protected static BatchQuery cqlBatchQuery(String cql, Value... values) {
    return BatchQuery.newBuilder().setCql(cql).setValues(valuesOf(values)).build();
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
            .addService(new GrpcService(persistence, executor, 2))
            .build();
    try {
      server.start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected void assertStatement(Prepared prepared, Statement statement, Value... values) {
    assertThat(statement).isInstanceOf(BoundStatement.class);
    assertThat(((BoundStatement) statement).preparedId()).isEqualTo(prepared.statementId);
    List<ByteBuffer> boundValues = statement.values();
    assertThat(boundValues).hasSize(values.length);
    for (int i = 0; i < values.length; ++i) {
      Column column = prepared.metadata.columns.get(i);
      assertThat(column.type().rawType()).isNotNull();
      Value actual =
          ValueCodecs.get(column.type().rawType()).decode(boundValues.get(i), column.type());
      assertThat(values[i]).isEqualTo(actual);
    }
  }

  static class AddClientHeadersInterceptor implements ClientInterceptor {

    private final Consumer<Metadata> addHeaders;

    AddClientHeadersInterceptor(Consumer<Metadata> addHeaders) {
      this.addHeaders = addHeaders;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          addHeaders.accept(headers);
          super.start(responseListener, headers);
        }
      };
    }
  }
}
