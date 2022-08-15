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

import io.grpc.Context;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.stargate.auth.AuthorizationService;
import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.Response;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridgeGrpc;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.schema.Keyspace;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class BridgeService extends StargateBridgeGrpc.StargateBridgeImplBase {

  public static final Context.Key<Persistence.Connection> CONNECTION_KEY =
      Context.key("connection");
  public static final Context.Key<Map<String, String>> HEADERS_KEY = Context.key("headers");
  public static final int DEFAULT_PAGE_SIZE = 100;
  public static final ConsistencyLevel DEFAULT_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM;
  public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY = ConsistencyLevel.SERIAL;

  private final Persistence persistence;
  private final AuthorizationService authorizationService;

  private final ScheduledExecutorService executor;
  private final int schemaAgreementRetries;
  private final Schema.SupportedFeaturesResponse supportedFeaturesResponse;

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
    this.supportedFeaturesResponse =
        Schema.SupportedFeaturesResponse.newBuilder()
            .setSecondaryIndexes(persistence.supportsSecondaryIndex())
            .setSai(persistence.supportsSAI())
            .setLoggedBatches(persistence.supportsLoggedBatches())
            .build();
  }

  @Override
  public void executeQuery(Query query, StreamObserver<Response> responseObserver) {
    SynchronizedStreamObserver<Response> synchronizedStreamObserver =
        new SynchronizedStreamObserver<>(responseObserver);
    new QueryHandler(
            query,
            CONNECTION_KEY.get(),
            persistence,
            executor,
            schemaAgreementRetries,
            synchronizedStreamObserver)
        .handle();
  }

  @Override
  public void executeQueryWithSchema(
      Schema.QueryWithSchema request,
      StreamObserver<Schema.QueryWithSchemaResponse> responseObserver) {
    String keyspaceName = request.getKeyspaceName();
    int keyspaceHash = request.getKeyspaceHash();
    String decoratedName =
        persistence.decorateKeyspaceName(keyspaceName, BridgeService.HEADERS_KEY.get());
    Keyspace keyspace = persistence.schema().keyspace(decoratedName);

    if (keyspace == null) {
      responseObserver.onNext(
          Schema.QueryWithSchemaResponse.newBuilder()
              .setNoKeyspace(Schema.QueryWithSchemaResponse.NoKeyspace.getDefaultInstance())
              .build());
      responseObserver.onCompleted();
    } else if (keyspace.schemaHashCode() != keyspaceHash) {
      try {
        responseObserver.onNext(
            Schema.QueryWithSchemaResponse.newBuilder()
                .setNewKeyspace(
                    SchemaHandler.buildKeyspaceDescription(keyspace, keyspaceName, decoratedName))
                .build());
        responseObserver.onCompleted();
      } catch (StatusException e) {
        responseObserver.onError(e);
      }
    } else {
      executeQuery(
          request.getQuery(),
          new StreamObserver<Response>() {
            @Override
            public void onNext(Response response) {
              responseObserver.onNext(
                  Schema.QueryWithSchemaResponse.newBuilder().setResponse(response).build());
            }

            @Override
            public void onError(Throwable throwable) {
              responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
              responseObserver.onCompleted();
            }
          });
    }
  }

  @Override
  public void executeBatch(Batch batch, StreamObserver<Response> responseObserver) {
    SynchronizedStreamObserver<Response> synchronizedStreamObserver =
        new SynchronizedStreamObserver<>(responseObserver);
    new BatchHandler(batch, CONNECTION_KEY.get(), persistence, synchronizedStreamObserver).handle();
  }

  @Override
  public void describeKeyspace(
      Schema.DescribeKeyspaceQuery request,
      StreamObserver<Schema.CqlKeyspaceDescribe> responseObserver) {
    executor.execute(() -> SchemaHandler.describeKeyspace(request, persistence, responseObserver));
  }

  @Override
  public void authorizeSchemaReads(
      Schema.AuthorizeSchemaReadsRequest request,
      StreamObserver<Schema.AuthorizeSchemaReadsResponse> responseObserver) {
    new AuthorizationHandler(request, CONNECTION_KEY.get(), authorizationService, responseObserver)
        .handle();
  }

  @Override
  public void getSupportedFeatures(
      Schema.SupportedFeaturesRequest request,
      StreamObserver<Schema.SupportedFeaturesResponse> responseObserver) {
    responseObserver.onNext(supportedFeaturesResponse);
    responseObserver.onCompleted();
  }

  static class ResponseAndTraceId {

    final @Nullable UUID tracingId;
    final Response.Builder responseBuilder;

    static ResponseAndTraceId from(Result result, Response.Builder responseBuilder) {
      return new ResponseAndTraceId(result.getTracingId(), responseBuilder);
    }

    private ResponseAndTraceId(@Nullable UUID tracingId, Response.Builder responseBuilder) {
      this.tracingId = tracingId;
      this.responseBuilder = responseBuilder;
    }

    public boolean tracingIdIsEmpty() {
      return tracingId == null || tracingId.toString().isEmpty();
    }
  }
}
