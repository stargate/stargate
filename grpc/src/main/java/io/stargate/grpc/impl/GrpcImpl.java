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
import io.grpc.stub.StreamObserver;
import io.stargate.auth.AuthenticationService;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.SimpleStatement;
import io.stargate.proto.StargateOuterClass.Request;
import io.stargate.proto.StargateOuterClass.ResultSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcImpl {
  private static final Logger logger = LoggerFactory.getLogger(GrpcImpl.class);

  private final Server server;

  private final Persistence persistence;
  private final Metrics metrics;
  private final AuthenticationService authentication;

  public GrpcImpl(Persistence persistence, Metrics metrics, AuthenticationService authentication) {
    this.persistence = persistence;
    this.metrics = metrics;
    this.authentication = authentication;

    // TODO: Make port configurable
    server = ServerBuilder.forPort(8090).addService(new StargateServer()).build();
  }

  public void start() {
    try {
      server.start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // TODO: What do we do here?
    persistence.setRpcReady(true);
  }

  public void stop() {
    try {
      server.shutdown().awaitTermination();
    } catch (InterruptedException e) {
      logger.error("Failed waiting for gRPC shutdown", e);
    }

    // TODO: Same as above. What do we do here?
    persistence.setRpcReady(false);
  }

  private class StargateServer extends io.stargate.proto.StargateGrpc.StargateImplBase {
    @Override
    public void query(Request request, StreamObserver<ResultSet> responseObserver) {
      // TODO: Handle parameters and result set
      long queryStartNanoTime = System.nanoTime();
      Parameters parameters = ImmutableParameters.builder().build();
      persistence
          .newConnection()
          .execute(new SimpleStatement(request.getQuery()), parameters, queryStartNanoTime)
          .whenComplete(
              (r, t) -> {
                if (t != null) {
                  responseObserver.onError(t);
                } else {
                  responseObserver.onNext(ResultSet.newBuilder().build());
                  responseObserver.onCompleted();
                }
              });
    }
  }
}
