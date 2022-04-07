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

import io.grpc.stub.StreamObserver;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.Persistence;
import io.stargate.grpc.service.GrpcService;
import io.stargate.grpc.service.SingleBatchHandler;
import io.stargate.grpc.service.SingleEnrichedExceptionHandler;
import io.stargate.grpc.service.SingleEnrichedQueryHandler;
import io.stargate.grpc.service.SingleExceptionHandler;
import io.stargate.grpc.service.SingleQueryHandler;
import io.stargate.grpc.service.SynchronizedStreamObserver;
import io.stargate.proto.BridgeQuery.EnrichedResponse;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateBridgeGrpc;
import java.util.concurrent.ScheduledExecutorService;

public class BridgeService extends StargateBridgeGrpc.StargateBridgeImplBase {

  private final Persistence persistence;
  private final AuthorizationService authorizationService;

  private final ScheduledExecutorService executor;
  private final int schemaAgreementRetries;

  public BridgeService(
      Persistence persistence,
      AuthorizationService authorizationService,
      ScheduledExecutorService executor) {
    this(persistence, authorizationService, executor, Persistence.SCHEMA_AGREEMENT_WAIT_RETRIES);
  }

  BridgeService(
      Persistence persistence,
      AuthorizationService authorizationService,
      ScheduledExecutorService executor,
      int schemaAgreementRetries) {
    this.persistence = persistence;
    this.authorizationService = authorizationService;
    this.executor = executor;
    this.schemaAgreementRetries = schemaAgreementRetries;
  }

  @Override
  public void executeQuery(Query query, StreamObserver<Response> responseObserver) {
    SynchronizedStreamObserver<Response> synchronizedStreamObserver =
        new SynchronizedStreamObserver<>(responseObserver);
    new SingleQueryHandler(
            query,
            GrpcService.CONNECTION_KEY.get(),
            persistence,
            executor,
            schemaAgreementRetries,
            synchronizedStreamObserver,
            new SingleExceptionHandler(synchronizedStreamObserver))
        .handle();
  }

  @Override
  public void executeEnrichedQuery(Query query, StreamObserver<EnrichedResponse> responseObserver) {
    SynchronizedStreamObserver<EnrichedResponse> synchronizedStreamObserver =
        new SynchronizedStreamObserver<>(responseObserver);
    new SingleEnrichedQueryHandler(
            query,
            GrpcService.CONNECTION_KEY.get(),
            persistence,
            executor,
            schemaAgreementRetries,
            synchronizedStreamObserver,
            new SingleEnrichedExceptionHandler(synchronizedStreamObserver))
        .handle();
  }

  @Override
  public void executeBatch(Batch batch, StreamObserver<Response> responseObserver) {
    SynchronizedStreamObserver<Response> synchronizedStreamObserver =
        new SynchronizedStreamObserver<>(responseObserver);
    new SingleBatchHandler(
            batch,
            GrpcService.CONNECTION_KEY.get(),
            persistence,
            synchronizedStreamObserver,
            new SingleExceptionHandler(synchronizedStreamObserver))
        .handle();
  }

  @Override
  public void describeKeyspace(
      Schema.DescribeKeyspaceQuery request,
      StreamObserver<Schema.CqlKeyspaceDescribe> responseObserver) {
    SchemaHandler.describeKeyspace(request, persistence, responseObserver);
  }

  @Override
  public void authorizeSchemaReads(
      Schema.AuthorizeSchemaReadsRequest request,
      StreamObserver<Schema.AuthorizeSchemaReadsResponse> responseObserver) {
    new AuthorizationHandler(
            request, GrpcService.CONNECTION_KEY.get(), authorizationService, responseObserver)
        .handle();
  }
}
