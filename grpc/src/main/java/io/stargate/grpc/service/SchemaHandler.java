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

import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.stargate.db.*;
import io.stargate.db.schema.*;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.proto.Schema.ColumnOrderBy;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.DescribeKeyspaceQuery;
import io.stargate.proto.Schema.DescribeTableQuery;
import org.jetbrains.annotations.NotNull;

class SchemaHandler {

  public static void describeKeyspace(
      DescribeKeyspaceQuery query,
      Persistence persistence,
      StreamObserver<CqlKeyspaceDescribe> responseObserver) {
    String decoratedKeyspace =
        persistence.decorateKeyspaceName(query.getKeyspaceName(), GrpcService.HEADERS_KEY.get());

    try {
      // TODO: check if null or access allowed?
      Keyspace keyspace = persistence.schema().keyspace(decoratedKeyspace);

      CqlKeyspaceDescribe.Builder describeResultBuilder = CqlKeyspaceDescribe.newBuilder();
      CqlKeyspace.Builder cqlKeyspaceBuilder = CqlKeyspace.newBuilder();
      cqlKeyspaceBuilder.setName(keyspace.name());

      // TODO: Persistence implementation doesn't set replication strategy
      //    Map<String, String> replication = new HashMap<String, String>(keyspace.replication());
      //    if (replication.containsKey("class")) {
      //      cqlKeyspaceBuilder.setReplicationStrategy(replication.remove("class"));
      //      for (String datacenter : replication.keySet()) {
      //        cqlKeyspaceBuilder.addReplicationFactors(
      //            CqlDatacenterReplication.newBuilder()
      //                .setDatacenterName(datacenter)
      //                .setReplicationFactor(Integer.parseInt(replication.get(datacenter)))
      //                .build());
      //      }
      //    }

      // TODO: Persistence implementation doesn't seem to actually set this
      if (keyspace.durableWrites().isPresent()) {
        cqlKeyspaceBuilder.putOptions("durable_writes", keyspace.durableWrites().get().toString());
      }

      describeResultBuilder.setCqlKeyspace(cqlKeyspaceBuilder.build());

      for (UserDefinedType udt : keyspace.userDefinedTypes()) {

        Udt.Builder udtBuilder = Udt.newBuilder();
        udtBuilder.setName(udt.name());
        udtBuilder.setFrozen(udt.isFrozen());
        for (Column column : udt.columns()) {
          udtBuilder.putFields(
              column.name(),
              ValuesHelper.convertType(ValuesHelper.columnTypeNotNull(column).rawType()));
        }
        describeResultBuilder.addTypes(udtBuilder.build());
      }

      for (Table table : keyspace.tables()) {
        describeResultBuilder.addTables(buildCqlTable(table));
      }

      // TODO: indexes and materialized views?

      responseObserver.onNext(describeResultBuilder.build());
      responseObserver.onCompleted();
    } catch (StatusException e) {
      responseObserver.onError(e);
    }
  }

  public static void describeTable(
      DescribeTableQuery query,
      Persistence persistence,
      StreamObserver<CqlTable> responseObserver) {
    String decoratedKeyspace =
        persistence.decorateKeyspaceName(query.getKeyspaceName(), GrpcService.HEADERS_KEY.get());

    try {
      Keyspace keyspace = persistence.schema().keyspace(decoratedKeyspace);
      Table table = keyspace.table(query.getTableName());
      responseObserver.onNext(buildCqlTable(table));
      responseObserver.onCompleted();
    } catch (StatusException e) {
      responseObserver.onError(e);
    }
  }

  @NotNull
  private static CqlTable buildCqlTable(Table table) throws StatusException {
    CqlTable.Builder cqlTableBuilder = CqlTable.newBuilder();
    cqlTableBuilder.setName(table.name());

    for (Column partitionKeyColumn : table.partitionKeyColumns()) {
      cqlTableBuilder.addPartitionKeyColumns(buildColumnSpec(partitionKeyColumn));
    }

    for (Column clusteringKeyColumn : table.clusteringKeyColumns()) {
      cqlTableBuilder.addClusteringKeyColumns(buildColumnSpec(clusteringKeyColumn));
      cqlTableBuilder.putClusteringKeyColumnSortOrders(
          clusteringKeyColumn.name(),
          ColumnOrderBy.forNumber(clusteringKeyColumn.order().ordinal()));
    }

    for (Column column : table.regularAndStaticColumns()) {
      if (column.kind().equals(Column.Kind.Static)) {
        cqlTableBuilder.addStaticColumns(buildColumnSpec(column));
      } else {
        cqlTableBuilder.addColumns(buildColumnSpec(column));
      }
    }

    // TODO: no table options in Table?
    // cqlTableBuilder.addTableOptions();

    return cqlTableBuilder.build();
  }

  @NotNull
  private static ColumnSpec buildColumnSpec(Column column) throws StatusException {
    ColumnSpec.Builder columnSpecBuilder = ColumnSpec.newBuilder();

    columnSpecBuilder.setName(column.name());
    columnSpecBuilder.setType(ValuesHelper.convertType(column.type()));

    return columnSpecBuilder.build();
  }
}
