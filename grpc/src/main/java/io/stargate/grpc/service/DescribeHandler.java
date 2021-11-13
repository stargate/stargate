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
import io.stargate.db.*;
import io.stargate.db.schema.*;
import io.stargate.proto.Schema.CqlColumn;
import io.stargate.proto.Schema.CqlDatacenterReplication;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.CqlType;
import io.stargate.proto.Schema.CqlUdtField;
import io.stargate.proto.Schema.CqlUserDefinedType;
import io.stargate.proto.Schema.DescribeKeyspaceQuery;
import io.stargate.proto.Schema.DescribeTableQuery;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

class DescribeHandler {

  private static final Map<Column.Type, CqlType> dbSchemaToGrpcType =
      new HashMap<Column.Type, CqlType>() {
        {
          put(Column.Type.Ascii, CqlType.ASCII);
          put(Column.Type.Bigint, CqlType.BIGINT);
          put(Column.Type.Blob, CqlType.BLOB);
          put(Column.Type.Boolean, CqlType.BOOLEAN);
          put(Column.Type.Counter, CqlType.COUNTER);
          put(Column.Type.Date, CqlType.DATE);
          put(Column.Type.Decimal, CqlType.DECIMAL);
          put(Column.Type.Double, CqlType.DOUBLE);
          put(Column.Type.Duration, CqlType.DURATION);
          put(Column.Type.Float, CqlType.FLOAT);
          put(Column.Type.Inet, CqlType.INET);
          put(Column.Type.Int, CqlType.INT);
          put(Column.Type.List, CqlType.LIST);
          put(Column.Type.LineString, CqlType.LINESTRING);
          put(Column.Type.Map, CqlType.MAP);
          put(Column.Type.Point, CqlType.POINT);
          put(Column.Type.Polygon, CqlType.POLYGON);
          put(Column.Type.Smallint, CqlType.SMALLINT);
          put(Column.Type.Text, CqlType.TEXT);
          put(Column.Type.Time, CqlType.TIME);
          put(Column.Type.Timestamp, CqlType.TIMESTAMP);
          put(Column.Type.Timeuuid, CqlType.TIMEUUID);
          put(Column.Type.Tinyint, CqlType.TINYINT);
          put(Column.Type.Tuple, CqlType.TUPLE);
          put(Column.Type.UDT, CqlType.UDT);
          put(Column.Type.Uuid, CqlType.UUID);
          put(Column.Type.Varchar, CqlType.VARCHAR);
          put(Column.Type.Varint, CqlType.VARINT);
        }
      };

  public static void describeKeyspace(
      DescribeKeyspaceQuery query,
      Persistence persistence,
      StreamObserver<CqlKeyspaceDescribe> responseObserver) {
    String decoratedKeyspace =
        persistence.decorateKeyspaceName(query.getKeyspaceName(), GrpcService.HEADERS_KEY.get());

    Keyspace keyspace = persistence.schema().keyspace(decoratedKeyspace);
    // TODO: check if null or access allowed?
    //    if (keyspace == null) {
    //      responseObserver.onError(Status.NOT_FOUND);
    //    }

    CqlKeyspaceDescribe.Builder describeResultBuilder = CqlKeyspaceDescribe.newBuilder();

    CqlKeyspace.Builder cqlKeyspaceBuilder = CqlKeyspace.newBuilder();
    cqlKeyspaceBuilder.setName(keyspace.name());

    // TODO: Persistence implementation doesn't seem to actually set these
    Map<String, String> replication = new HashMap<String, String>(keyspace.replication());
    if (replication.containsKey("class")) {
      cqlKeyspaceBuilder.setReplicationStrategy(replication.remove("class"));
      for (String datacenter : replication.keySet()) {
        cqlKeyspaceBuilder.addReplicationFactors(
            CqlDatacenterReplication.newBuilder()
                .setDatacenterName(datacenter)
                .setReplicationFactor(Integer.parseInt(replication.get(datacenter)))
                .build());
      }
    }

    // TODO: Persistence implementation doesn't seem to actually set this
    if (keyspace.durableWrites().isPresent()) {
      cqlKeyspaceBuilder.setDurableWrites(keyspace.durableWrites().get().booleanValue());
    }

    describeResultBuilder.setCqlKeyspace(cqlKeyspaceBuilder.build());

    for (UserDefinedType udt : keyspace.userDefinedTypes()) {
      CqlUserDefinedType.Builder cqlUdtBuilder = CqlUserDefinedType.newBuilder();

      cqlUdtBuilder.setName(udt.name());

      for (Column udtColumn : udt.columns()) {
        CqlUdtField.Builder udtColumnBuilder = CqlUdtField.newBuilder();
        udtColumnBuilder.setName(udtColumn.name());
        udtColumnBuilder.setFrozen(udtColumn.type().isFrozen()); // TODO check
        udtColumnBuilder.setType(dbSchemaToGrpcType.get(udtColumn.type().rawType()));
        if (udtColumn.type().rawType() == Column.Type.UDT) { // TODO: and for collection types?
          udtColumnBuilder.setCqlTypeName(udtColumn.type().name()); // TODO this is not right
        }

        cqlUdtBuilder.addFields(udtColumnBuilder.build());
      }

      describeResultBuilder.addTypes(cqlUdtBuilder.build());
    }

    for (Table table : keyspace.tables()) {
      describeResultBuilder.addTables(buildCqlTable(table));
    }

    // TODO: indexes and materialized views?

    responseObserver.onNext(describeResultBuilder.build());
    responseObserver.onCompleted();
  }

  public static void describeTable(
      DescribeTableQuery query,
      Persistence persistence,
      StreamObserver<CqlTable> responseObserver) {
    String decoratedKeyspace =
        persistence.decorateKeyspaceName(query.getKeyspaceName(), GrpcService.HEADERS_KEY.get());

    Keyspace keyspace = persistence.schema().keyspace(decoratedKeyspace);
    // TODO: check if null or access allowed?
    //    if (keyspace == null) {
    //      responseObserver.onError(Status.NOT_FOUND);
    //    }

    Table table = keyspace.table(query.getTableName());
    // TODO: check?

    responseObserver.onNext(buildCqlTable(table));
    responseObserver.onCompleted();
  }

  @NotNull
  private static CqlTable buildCqlTable(Table table) {
    CqlTable.Builder cqlTableBuilder = CqlTable.newBuilder();
    cqlTableBuilder.setName(table.name());

    for (Column column : table.columns()) {
      CqlColumn.Builder cqlColumnBuilder = CqlColumn.newBuilder();

      // TODO: check for conversion error?
      cqlColumnBuilder.setName(column.name());
      cqlColumnBuilder.setType(dbSchemaToGrpcType.get(column.type().rawType()));

      //        switch (column.type().rawType()) {
      //          case List:
      //          case Set:
      //            CqlComplexColumnType.Builder complexColumnTypeBuilder =
      // CqlComplexColumnType.newBuilder();
      //
      // complexColumnTypeBuilder.setType(dbSchemaToGrpcType.get(column.type().parameters().get(0)));
      //            cqlColumnBuilder.addComplexColumnTypes(column.type().parameters().get(0));
      //            break;
      //          case Map:
      //            cqlColumnBuilder.setType(CqlType.MAP);
      //            // TODO: set enclosed type
      //            break;
      //          case Tuple:
      //            cqlColumnBuilder.setType(CqlType.TUPLE);
      //            break;
      //          case UDT:
      //            cqlColumnBuilder.setType(CqlType.UDT);
      //            // TODO: set type name
      //            break;
      //          default:
      //            break;
      //        }

      cqlTableBuilder.addColumns(cqlColumnBuilder.build());
    }

    // TODO: no table options in Table?
    // cqlTableBuilder.addTableOptions();

    return cqlTableBuilder.build();
  }
}
