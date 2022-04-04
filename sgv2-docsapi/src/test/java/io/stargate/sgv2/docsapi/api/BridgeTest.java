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

package io.stargate.sgv2.docsapi.api;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.stargate.proto.StargateBridgeGrpc;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/**
 * Provides a basic support for mocking the Bridge implementation.
 */
public class BridgeTest {

    protected StargateBridgeGrpc.StargateBridgeImplBase bridgeImplBase;

    Server server;

    ManagedChannel channel;

    @ConfigProperty(name = "quarkus.grpc.clients.bridge.port")
    int bridgePort;

    @BeforeEach
    public void init() throws Exception{
        // init mock
        bridgeImplBase = mock(StargateBridgeGrpc.StargateBridgeImplBase.class);

        // set up the server that runs on the target bridge port
        SocketAddress address = new InetSocketAddress(bridgePort);
        server = NettyServerBuilder.forAddress(address).directExecutor().addService(bridgeImplBase).build();
        server.start();
        channel = NettyChannelBuilder.forAddress(address).usePlaintext().build();
    }

    @AfterEach
    public void tearDown() throws Exception {
        channel.shutdown();
        server.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

}
