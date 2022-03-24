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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Keyspace;
import io.stargate.grpc.service.GrpcService;
import io.stargate.grpc.service.SingleBatchHandler;
import io.stargate.grpc.service.SingleExceptionHandler;
import io.stargate.grpc.service.SingleQueryHandler;
import io.stargate.grpc.service.SynchronizedStreamObserver;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateBridgeGrpc;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BridgeService extends StargateBridgeGrpc.StargateBridgeImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(BridgeService.class);

  private static final Schema.CqlKeyspaceDescribe EMPTY_KEYSPACE_DESCRIPTION =
      Schema.CqlKeyspaceDescribe.newBuilder().build();

  private final Persistence persistence;
  private final AuthorizationService authorizationService;

  private final ScheduledExecutorService executor;
  private final int schemaAgreementRetries;

  private final LoadingCache<Keyspace, Schema.CqlKeyspaceDescribe> descriptionCache =
      Caffeine.newBuilder()
          // TODO adjust size and TTL
          .maximumSize(1000)
          .build(SchemaHandler::buildKeyspaceDescription);

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

    String decoratedName =
        persistence.decorateKeyspaceName(request.getKeyspaceName(), GrpcService.HEADERS_KEY.get());

    Keyspace keyspace = persistence.schema().keyspace(decoratedName);
    if (keyspace == null) {
      responseObserver.onError(
          Status.NOT_FOUND.withDescription("Keyspace not found").asException());
    } else if (request.hasHash() && request.getHash().getValue() == keyspace.hashCode()) {
      // Client already has the latest version, don't resend
      responseObserver.onNext(EMPTY_KEYSPACE_DESCRIPTION);
      responseObserver.onCompleted();
    } else {
      try {
        Schema.CqlKeyspaceDescribe description = descriptionCache.get(keyspace);
        responseObserver.onNext(description);
        responseObserver.onCompleted();
      } catch (CompletionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof StatusException) {
          responseObserver.onError(e);
        } else {
          LOG.error("Internal error while building keyspace description", cause);
          responseObserver.onError(
              Status.INTERNAL
                  .withDescription("Internal error while building keyspace description")
                  .asException());
        }
        descriptionCache.invalidate(keyspace);
      }
    }
  }

  @Override
  public void describeTable(
      Schema.DescribeTableQuery request, StreamObserver<Schema.CqlTable> responseObserver) {
    SchemaHandler.describeTable(request, persistence, responseObserver);
  }

  @Override
  public void getSchemaNotifications(
      Schema.GetSchemaNotificationsParams request,
      StreamObserver<Schema.SchemaNotification> responseObserver) {
    new SchemaNotificationsHandler(persistence, responseObserver).handle();
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
