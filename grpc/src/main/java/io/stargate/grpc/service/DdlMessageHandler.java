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

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.SimpleStatement;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableListType;
import io.stargate.db.schema.ImmutableMapType;
import io.stargate.db.schema.ImmutableSetType;
import io.stargate.db.schema.ImmutableTupleType;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import io.stargate.proto.Schema;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

/** Common logic for DDL statements like CREATE TABLE, ALTER TYPE, etc. */
abstract class DdlMessageHandler<MessageT extends GeneratedMessageV3>
    extends MessageHandler<MessageT> {

  protected final Persistence.Connection connection;
  private final StreamObserver<Response> responseObserver;
  protected final String query;
  private final SchemaAgreementHelper schemaAgreementHelper;

  protected DdlMessageHandler(
      MessageT message,
      Persistence.Connection connection,
      Persistence persistence,
      TypedValue.Codec valueCodec,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<Response> responseObserver) {
    super(message, new SingleExceptionHandler(responseObserver));
    this.connection = connection;
    this.responseObserver = responseObserver;

    // Note that, due to executor being null, the returned queries are not executable. This is only
    // intended to construct CQL query strings.
    QueryBuilder queryBuilder = new QueryBuilder(persistence.schema(), valueCodec, null);

    this.query = buildQuery(message, persistence, queryBuilder);
    this.schemaAgreementHelper =
        new SchemaAgreementHelper(connection, schemaAgreementRetries, executor);
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  protected abstract String buildQuery(
      MessageT message, Persistence persistence, QueryBuilder queryBuilder);

  @Override
  protected CompletionStage<Response> executeQuery() {
    long queryStartNanoTime = System.nanoTime();
    try {
      return connection
          .execute(
              new SimpleStatement(query),
              DdlMessageHandler.makeParameters(connection.clientInfo()),
              queryStartNanoTime)
          .thenCompose(this::buildResponse);
    } catch (Exception e) {
      return failedFuture(e, false);
    }
  }

  private CompletionStage<Response> buildResponse(Result result) {
    Response.Builder responseBuilder = makeResponseBuilder(result);
    switch (result.kind) {
      case Void:
        return CompletableFuture.completedFuture(responseBuilder.build());
      case SchemaChange:
        return schemaAgreementHelper
            .waitForAgreement()
            .thenApply(
                __ -> {
                  SchemaChange schemaChange = buildSchemaChange((Result.SchemaChange) result);
                  responseBuilder.setSchemaChange(schemaChange);
                  return responseBuilder.setSchemaChange(schemaChange).build();
                });
      default:
        return failedFuture(
            Status.INTERNAL.withDescription("Unhandled result kind").asException(), false);
    }
  }

  /**
   * Converts a type reference in an incoming DDL call. Note that UDTs are translated as type
   * references, so this method not be used to convert the root UDT for a UDT creation.
   */
  protected static Column.ColumnType convertType(QueryOuterClass.TypeSpec typeProto) {
    if (typeProto.hasList()) {
      QueryOuterClass.TypeSpec.List listProto = typeProto.getList();
      return ImmutableListType.builder()
          .addParameters(DdlMessageHandler.convertType(listProto.getElement()))
          .isFrozen(listProto.getFrozen())
          .build();
    }
    if (typeProto.hasSet()) {
      QueryOuterClass.TypeSpec.Set setProto = typeProto.getSet();
      return ImmutableSetType.builder()
          .addParameters(DdlMessageHandler.convertType(setProto.getElement()))
          .isFrozen(setProto.getFrozen())
          .build();
    }
    if (typeProto.hasMap()) {
      QueryOuterClass.TypeSpec.Map mapProto = typeProto.getMap();
      return ImmutableMapType.builder()
          .addParameters(
              DdlMessageHandler.convertType(mapProto.getKey()),
              DdlMessageHandler.convertType(mapProto.getValue()))
          .isFrozen(mapProto.getFrozen())
          .build();
    }
    if (typeProto.hasUdt()) {
      QueryOuterClass.TypeSpec.Udt udtProto = typeProto.getUdt();
      // Shallow reference:
      return ImmutableUserDefinedType.reference(udtProto.getName()).frozen(udtProto.getFrozen());
    }
    if (typeProto.hasTuple()) {
      QueryOuterClass.TypeSpec.Tuple tupleProto = typeProto.getTuple();
      return ImmutableTupleType.builder()
          .addParameters(
              tupleProto.getElementsList().stream()
                  .map(e -> DdlMessageHandler.convertType(e))
                  .toArray(Column.ColumnType[]::new))
          .build();
    }
    switch (typeProto.getBasic()) {
      case ASCII:
        return Column.Type.Ascii;
      case BIGINT:
        return Column.Type.Bigint;
      case BLOB:
        return Column.Type.Blob;
      case BOOLEAN:
        return Column.Type.Boolean;
      case COUNTER:
        return Column.Type.Counter;
      case DECIMAL:
        return Column.Type.Decimal;
      case DOUBLE:
        return Column.Type.Double;
      case FLOAT:
        return Column.Type.Float;
      case INT:
        return Column.Type.Int;
      case TEXT:
      case VARCHAR:
        return Column.Type.Text;
      case TIMESTAMP:
        return Column.Type.Timestamp;
      case UUID:
        return Column.Type.Uuid;
      case VARINT:
        return Column.Type.Varint;
      case TIMEUUID:
        return Column.Type.Timeuuid;
      case INET:
        return Column.Type.Inet;
      case DATE:
        return Column.Type.Date;
      case TIME:
        return Column.Type.Time;
      case SMALLINT:
        return Column.Type.Smallint;
      case TINYINT:
        return Column.Type.Tinyint;
      case DURATION:
        return Column.Type.Duration;
      case LINESTRING:
        return Column.Type.LineString;
      case POINT:
        return Column.Type.Point;
      case POLYGON:
        return Column.Type.Polygon;
      case CUSTOM:
        // Can't materialize the type without a class name.
        // TODO should we fix this in the proto file?
        throw new IllegalArgumentException("CUSTOM types are not supported here");
      default:
        throw new IllegalArgumentException("Unsupported type " + typeProto.getBasic());
    }
  }

  protected static Column.Order convertOrder(Schema.ColumnOrderBy orderProto) {
    switch (orderProto) {
      case ASC:
        return Column.Order.ASC;
      case DESC:
        return Column.Order.DESC;
      default:
        throw new IllegalArgumentException("Unsupported clustering order " + orderProto);
    }
  }

  protected static Parameters makeParameters(Optional<ClientInfo> clientInfo) {
    if (clientInfo.isPresent()) {
      Map<String, ByteBuffer> customPayload = new HashMap<>();
      clientInfo.get().storeAuthenticationData(customPayload);
      return ImmutableParameters.builder().customPayload(customPayload).build();
    } else {
      return Parameters.defaults();
    }
  }
}
