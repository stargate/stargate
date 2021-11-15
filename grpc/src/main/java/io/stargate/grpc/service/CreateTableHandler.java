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

import com.datastax.dse.driver.api.core.type.DseDataTypes;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateTable;
import io.grpc.stub.StreamObserver;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.SimpleStatement;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.Schema;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.CqlTableCreate;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

class CreateTableHandler extends MessageHandler<CqlTableCreate> {

  private final Persistence.Connection connection;
  private final Persistence persistence;
  private final String query;

  CreateTableHandler(
      CqlTableCreate createTable,
      Persistence.Connection connection,
      Persistence persistence,
      StreamObserver<QueryOuterClass.Response> responseObserver) {
    super(createTable, responseObserver);
    this.connection = connection;
    this.persistence = persistence;
    this.query = buildQuery(createTable);
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

  private String buildQuery(CqlTableCreate createProto) {
    String keyspaceName =
        persistence.decorateKeyspaceName(
            createProto.getKeyspaceName(), GrpcService.HEADERS_KEY.get());
    CqlTable tableProto = createProto.getTable();
    DefaultCreateTable builder =
        (DefaultCreateTable) SchemaBuilder.createTable(keyspaceName, tableProto.getName());
    if (createProto.getIfNotExists()) {
      builder = (DefaultCreateTable) builder.ifNotExists();
    }
    for (ColumnSpec columnProto : tableProto.getPartitionKeyColumnsList()) {
      builder =
          (DefaultCreateTable)
              builder.withPartitionKey(columnProto.getName(), convertType(columnProto.getType()));
    }
    for (ColumnSpec columnProto : tableProto.getClusteringKeyColumnsList()) {
      builder =
          (DefaultCreateTable)
              builder.withClusteringColumn(
                  columnProto.getName(), convertType(columnProto.getType()));
    }
    for (ColumnSpec columnProto : tableProto.getStaticColumnsList()) {
      builder =
          (DefaultCreateTable)
              builder.withStaticColumn(columnProto.getName(), convertType(columnProto.getType()));
    }
    for (ColumnSpec columnProto : tableProto.getColumnsList()) {
      builder =
          (DefaultCreateTable)
              builder.withColumn(columnProto.getName(), convertType(columnProto.getType()));
    }
    for (Map.Entry<String, Schema.ColumnOrderBy> entry :
        tableProto.getClusteringOrdersMap().entrySet()) {
      builder =
          (DefaultCreateTable)
              builder.withClusteringOrder(entry.getKey(), convertOrder(entry.getValue()));
    }
    // TODO handle table options
    return builder.asCql();
  }

  /**
   * Converts a type reference in an incoming DDL call. Note that UDTs are translated as type
   * references, so this method not be used to convert the root UDT for a UDT creation.
   */
  private static DataType convertType(TypeSpec typeProto) {
    if (typeProto.hasList()) {
      TypeSpec.List listProto = typeProto.getList();
      return DataTypes.listOf(convertType(listProto.getElement()), listProto.getFrozen());
    }
    if (typeProto.hasSet()) {
      TypeSpec.Set setProto = typeProto.getSet();
      return DataTypes.setOf(convertType(setProto.getElement()), setProto.getFrozen());
    }
    if (typeProto.hasMap()) {
      TypeSpec.Map mapProto = typeProto.getMap();
      return DataTypes.mapOf(
          convertType(mapProto.getKey()), convertType(mapProto.getValue()), mapProto.getFrozen());
    }
    if (typeProto.hasUdt()) {
      TypeSpec.Udt udtProto = typeProto.getUdt();
      // Shallow reference:
      return SchemaBuilder.udt(udtProto.getName(), udtProto.getFrozen());
    }
    if (typeProto.hasTuple()) {
      TypeSpec.Tuple tupleProto = typeProto.getTuple();
      return DataTypes.tupleOf(
          tupleProto.getElementsList().stream().map(e -> convertType(e)).toArray(DataType[]::new));
    }
    switch (typeProto.getBasic()) {
      case ASCII:
        return DataTypes.ASCII;
      case BIGINT:
        return DataTypes.BIGINT;
      case BLOB:
        return DataTypes.BLOB;
      case BOOLEAN:
        return DataTypes.BOOLEAN;
      case COUNTER:
        return DataTypes.COUNTER;
      case DECIMAL:
        return DataTypes.DECIMAL;
      case DOUBLE:
        return DataTypes.DOUBLE;
      case FLOAT:
        return DataTypes.FLOAT;
      case INT:
        return DataTypes.INT;
      case TEXT:
      case VARCHAR:
        return DataTypes.TEXT;
      case TIMESTAMP:
        return DataTypes.TIMESTAMP;
      case UUID:
        return DataTypes.UUID;
      case VARINT:
        return DataTypes.VARINT;
      case TIMEUUID:
        return DataTypes.TIMEUUID;
      case INET:
        return DataTypes.INET;
      case DATE:
        return DataTypes.DATE;
      case TIME:
        return DataTypes.TIME;
      case SMALLINT:
        return DataTypes.SMALLINT;
      case TINYINT:
        return DataTypes.TINYINT;
      case DURATION:
        return DataTypes.DURATION;
      case LINESTRING:
        return DseDataTypes.LINE_STRING;
      case POINT:
        return DseDataTypes.POINT;
      case POLYGON:
        return DseDataTypes.POLYGON;
      case CUSTOM:
        // Can't materialize the type without a class name.
        // TODO should we fix this in the proto file?
        throw new IllegalArgumentException("CUSTOM types are not supported here");
      default:
        throw new IllegalArgumentException("Unsupported type " + typeProto.getBasic());
    }
  }

  private static ClusteringOrder convertOrder(Schema.ColumnOrderBy orderProto) {
    switch (orderProto) {
      case ASC:
        return ClusteringOrder.ASC;
      case DESC:
        return ClusteringOrder.DESC;
      default:
        throw new IllegalArgumentException("Unsupported clustering order " + orderProto);
    }
  }

  private Parameters makeParameters(Optional<ClientInfo> clientInfo) {
    if (clientInfo.isPresent()) {
      Map<String, ByteBuffer> customPayload = new HashMap<>();
      clientInfo.get().storeAuthenticationData(customPayload);
      return ImmutableParameters.builder().customPayload(customPayload).build();
    } else {
      return Parameters.defaults();
    }
  }
}
