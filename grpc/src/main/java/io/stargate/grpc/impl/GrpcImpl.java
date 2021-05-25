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
package io.stargate.grpc.impl;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.stargate.auth.AuthenticationService;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import io.stargate.grpc.service.Service;
import io.stargate.grpc.service.interceptors.AuthenticationInterceptor;
import io.stargate.grpc.service.interceptors.RemoteAddressInterceptor;
import io.stargate.grpc.service.interceptors.metrics.MetricCollectingServerInterceptor;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcImpl {
  private static final Logger logger = LoggerFactory.getLogger(GrpcImpl.class);

  private static final int PORT = Integer.getInteger("stargate.grpc.port", 8090);

  private final Server server;

  public GrpcImpl(
      Persistence persistence, Metrics metrics, AuthenticationService authenticationService) {
    server =
        ServerBuilder.forPort(PORT)
            .intercept(new AuthenticationInterceptor(authenticationService))
            .intercept(new RemoteAddressInterceptor())
            .intercept(new MetricCollectingServerInterceptor(metrics.getMeterRegistry()))
            .addService(new Service(persistence, metrics))
            .build();
  }

  public void start() {
    try {
      server.start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void stop() {
    try {
      server.shutdown().awaitTermination();
    } catch (InterruptedException e) {
      logger.error("Failed waiting for gRPC shutdown", e);
    }
  }
}
