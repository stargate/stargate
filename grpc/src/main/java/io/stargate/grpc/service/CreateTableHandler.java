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

import io.grpc.stub.StreamObserver;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.SimpleStatement;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableListType;
import io.stargate.db.schema.ImmutableMapType;
import io.stargate.db.schema.ImmutableSetType;
import io.stargate.db.schema.ImmutableTupleType;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.Schema;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.CqlTableCreate;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

class CreateTableHandler extends MessageHandler<CqlTableCreate> {

  private final Persistence.Connection connection;
  private final String query;

  CreateTableHandler(
      CqlTableCreate createTable,
      Persistence.Connection connection,
      Persistence persistence,
      QueryBuilder queryBuilder,
      StreamObserver<QueryOuterClass.Response> responseObserver) {
    super(createTable, responseObserver);
    this.connection = connection;
    this.query = buildQuery(createTable, persistence, queryBuilder);
  }

  @Override
  protected void validate() {
    // intentionally empty
  }

  @Override
  protected CompletionStage<QueryOuterClass.Response> executeQuery() {
    long queryStartNanoTime = System.nanoTime();
    try {
      return connection
          .execute(
              new SimpleStatement(query),
              makeParameters(connection.clientInfo()),
              queryStartNanoTime)
          .thenApply(result -> makeResponseBuilder(result).build());
    } catch (Exception e) {
      return failedFuture(e, false);
    }
  }

  private String buildQuery(
      CqlTableCreate createProto, Persistence persistence, QueryBuilder queryBuilder) {
    String keyspaceName =
        persistence.decorateKeyspaceName(
            createProto.getKeyspaceName(), GrpcService.HEADERS_KEY.get());
    CqlTable tableProto = createProto.getTable();

    List<Column> columns = new ArrayList<>();
    for (ColumnSpec columnProto : tableProto.getPartitionKeyColumnsList()) {
      columns.add(
          ImmutableColumn.create(
              columnProto.getName(), Column.Kind.PartitionKey, convertType(columnProto.getType())));
    }
    for (ColumnSpec columnProto : tableProto.getClusteringKeyColumnsList()) {
      String name = columnProto.getName();
      Schema.ColumnOrderBy orderProto =
          tableProto.getClusteringOrdersMap().getOrDefault(name, Schema.ColumnOrderBy.ASC);
      columns.add(
          ImmutableColumn.create(
              name,
              Column.Kind.Clustering,
              convertType(columnProto.getType()),
              convertOrder(orderProto)));
    }
    for (ColumnSpec columnProto : tableProto.getStaticColumnsList()) {
      columns.add(
          ImmutableColumn.create(
              columnProto.getName(), Column.Kind.Static, convertType(columnProto.getType())));
    }
    for (ColumnSpec columnProto : tableProto.getColumnsList()) {
      columns.add(
          ImmutableColumn.create(
              columnProto.getName(), Column.Kind.Regular, convertType(columnProto.getType())));
    }

    return queryBuilder
        .create()
        .table(keyspaceName, tableProto.getName())
        .ifNotExists(createProto.getIfNotExists())
        .column(columns)
        // TODO handle table options
        .build()
        .bind()
        .queryString();
  }

  /**
   * Converts a type reference in an incoming DDL call. Note that UDTs are translated as type
   * references, so this method not be used to convert the root UDT for a UDT creation.
   */
  private static Column.ColumnType convertType(TypeSpec typeProto) {
    if (typeProto.hasList()) {
      TypeSpec.List listProto = typeProto.getList();
      return ImmutableListType.builder()
          .addParameters(convertType(listProto.getElement()))
          .isFrozen(listProto.getFrozen())
          .build();
    }
    if (typeProto.hasSet()) {
      TypeSpec.Set setProto = typeProto.getSet();
      return ImmutableSetType.builder()
          .addParameters(convertType(setProto.getElement()))
          .isFrozen(setProto.getFrozen())
          .build();
    }
    if (typeProto.hasMap()) {
      TypeSpec.Map mapProto = typeProto.getMap();
      return ImmutableMapType.builder()
          .addParameters(convertType(mapProto.getKey()), convertType(mapProto.getValue()))
          .isFrozen(mapProto.getFrozen())
          .build();
    }
    if (typeProto.hasUdt()) {
      TypeSpec.Udt udtProto = typeProto.getUdt();
      // Shallow reference:
      return ImmutableUserDefinedType.reference(udtProto.getName()).frozen(udtProto.getFrozen());
    }
    if (typeProto.hasTuple()) {
      TypeSpec.Tuple tupleProto = typeProto.getTuple();
      return ImmutableTupleType.builder()
          .addParameters(
              tupleProto.getElementsList().stream()
                  .map(e -> convertType(e))
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

  private static Column.Order convertOrder(Schema.ColumnOrderBy orderProto) {
    switch (orderProto) {
      case ASC:
        return Column.Order.ASC;
      case DESC:
        return Column.Order.DESC;
      default:
        throw new IllegalArgumentException("Unsupported clustering order " + orderProto);
    }
  }

  private static Parameters makeParameters(Optional<ClientInfo> clientInfo) {
    if (clientInfo.isPresent()) {
      Map<String, ByteBuffer> customPayload = new HashMap<>();
      clientInfo.get().storeAuthenticationData(customPayload);
      return ImmutableParameters.builder().customPayload(customPayload).build();
    } else {
      return Parameters.defaults();
    }
  }
}
