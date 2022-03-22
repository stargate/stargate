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

import com.google.protobuf.StringValue;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.db.query.builder.Replication;
import io.stargate.db.schema.CollectionIndexingType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.MaterializedView;
import io.stargate.db.schema.SecondaryIndex;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.grpc.service.GrpcService;
import io.stargate.grpc.service.ValuesHelper;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.proto.Schema;
import io.stargate.proto.Schema.ColumnOrderBy;
import io.stargate.proto.Schema.CqlIndex;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.CqlMaterializedView;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.DescribeKeyspaceQuery;
import io.stargate.proto.Schema.DescribeTableQuery;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

class SchemaHandler {

  public static void describeKeyspace(
      DescribeKeyspaceQuery query,
      Persistence persistence,
      StreamObserver<CqlKeyspaceDescribe> responseObserver) {
    try {
      String decoratedKeyspace =
          persistence.decorateKeyspaceName(query.getKeyspaceName(), GrpcService.HEADERS_KEY.get());
      Keyspace keyspace = persistence.schema().keyspace(decoratedKeyspace);
      if (keyspace == null) {
        throw Status.NOT_FOUND.withDescription("Keyspace not found").asException();
      }
      responseObserver.onNext(buildKeyspaceDescription(keyspace));
      responseObserver.onCompleted();
    } catch (StatusException e) {
      responseObserver.onError(e);
    }
  }

  static CqlKeyspaceDescribe buildKeyspaceDescription(Keyspace keyspace) throws StatusException {

    CqlKeyspaceDescribe.Builder describeResultBuilder = CqlKeyspaceDescribe.newBuilder();
    CqlKeyspace.Builder cqlKeyspaceBuilder = CqlKeyspace.newBuilder();
    cqlKeyspaceBuilder.setName(keyspace.name());

    Map<String, String> replication = new LinkedHashMap<>(keyspace.replication());
    if (replication.containsKey("class")) {
      String strategyName = replication.remove("class");
      if (strategyName.endsWith("SimpleStrategy")) {
        cqlKeyspaceBuilder.putOptions("replication", Replication.simpleStrategy(1).toString());
      } else if (strategyName.endsWith("NetworkTopologyStrategy")) {
        Map<String, Integer> replicationMap = new HashMap<String, Integer>();
        for (Map.Entry<String, String> entry : replication.entrySet()) {
          replicationMap.put(entry.getKey(), Integer.parseInt(entry.getValue()));
        }

        cqlKeyspaceBuilder.putOptions(
            "replication", Replication.networkTopologyStrategy(replicationMap).toString());
      }
    }

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
            column.name(), ValuesHelper.convertType(ValuesHelper.columnTypeNotNull(column)));
      }
      describeResultBuilder.addTypes(udtBuilder.build());
    }

    for (Table table : keyspace.tables()) {
      describeResultBuilder.addTables(buildCqlTable(table));
    }

    return describeResultBuilder.build();
  }

  public static void describeTable(
      DescribeTableQuery query,
      Persistence persistence,
      StreamObserver<CqlTable> responseObserver) {
    String decoratedKeyspace =
        persistence.decorateKeyspaceName(query.getKeyspaceName(), GrpcService.HEADERS_KEY.get());

    try {
      Keyspace keyspace = persistence.schema().keyspace(decoratedKeyspace);
      if (keyspace == null) {
        throw Status.NOT_FOUND.withDescription("Keyspace not found").asException();
      }
      Table table = keyspace.table(query.getTableName());
      if (table == null) {
        throw Status.NOT_FOUND.withDescription("Table not found").asException();
      }

      responseObserver.onNext(buildCqlTable(table));
      responseObserver.onCompleted();
    } catch (StatusException e) {
      responseObserver.onError(e);
    }
  }

  @NotNull
  private static CqlTable buildCqlTable(Table table) throws StatusException {
    CqlTable.Builder cqlTableBuilder = CqlTable.newBuilder().setName(table.name());

    for (Column partitionKeyColumn : table.partitionKeyColumns()) {
      cqlTableBuilder.addPartitionKeyColumns(buildColumnSpec(partitionKeyColumn));
    }

    for (Column clusteringKeyColumn : table.clusteringKeyColumns()) {
      cqlTableBuilder.addClusteringKeyColumns(buildColumnSpec(clusteringKeyColumn));
      cqlTableBuilder.putClusteringOrders(
          clusteringKeyColumn.name(),
          ColumnOrderBy.forNumber(clusteringKeyColumn.order().ordinal()));
    }

    for (Column column : table.regularAndStaticColumns()) {
      if (column.kind() == Column.Kind.Static) {
        cqlTableBuilder.addStaticColumns(buildColumnSpec(column));
      } else {
        cqlTableBuilder.addColumns(buildColumnSpec(column));
      }
    }

    // TODO: no table options in Table?
    // cqlTableBuilder.putOptions(...);

    for (Index index : table.indexes()) {
      if (index instanceof SecondaryIndex) {
        cqlTableBuilder.addIndexes(buildSecondaryIndex((SecondaryIndex) index));
      } else if (index instanceof MaterializedView) {
        cqlTableBuilder.addMaterializedViews(buildMaterializedView((MaterializedView) index));
      }
    }

    return cqlTableBuilder.build();
  }

  private static CqlIndex buildSecondaryIndex(SecondaryIndex index) {
    CqlIndex.Builder builder =
        CqlIndex.newBuilder().setName(index.name()).setColumnName(index.column().name());
    CollectionIndexingType indexingType = index.indexingType();
    if (indexingType.indexKeys()) {
      builder.setIndexingType(Schema.IndexingType.KEYS);
    } else if (indexingType.indexValues()) {
      builder.setIndexingType(Schema.IndexingType.VALUES_);
    } else if (indexingType.indexEntries()) {
      builder.setIndexingType(Schema.IndexingType.ENTRIES);
    } else if (indexingType.indexFull()) {
      builder.setIndexingType(Schema.IndexingType.FULL);
    }
    builder.setCustom(index.isCustom());
    String indexingClass = index.indexingClass();
    if (indexingClass != null) {
      builder.setIndexingClass(StringValue.of(indexingClass));
    }
    builder.putAllOptions(index.indexingOptions());
    return builder.build();
  }

  private static CqlMaterializedView buildMaterializedView(MaterializedView materializedView)
      throws StatusException {
    CqlMaterializedView.Builder builder =
        CqlMaterializedView.newBuilder().setName(materializedView.name());

    for (Column partitionKeyColumn : materializedView.partitionKeyColumns()) {
      builder.addPartitionKeyColumns(buildColumnSpec(partitionKeyColumn));
    }

    for (Column clusteringKeyColumn : materializedView.clusteringKeyColumns()) {
      builder.addClusteringKeyColumns(buildColumnSpec(clusteringKeyColumn));
      builder.putClusteringOrders(
          clusteringKeyColumn.name(),
          ColumnOrderBy.forNumber(clusteringKeyColumn.order().ordinal()));
    }

    for (Column column : materializedView.regularAndStaticColumns()) {
      builder.addColumns(buildColumnSpec(column));
    }

    // TODO: no options in MaterializedView?
    // builder.putOptions(materializedView.TBD);

    return builder.build();
  }

  @NotNull
  private static ColumnSpec buildColumnSpec(Column column) throws StatusException {
    ColumnSpec.Builder columnSpecBuilder = ColumnSpec.newBuilder();

    columnSpecBuilder.setName(column.name());
    columnSpecBuilder.setType(ValuesHelper.convertType(column.type()));

    return columnSpecBuilder.build();
  }
}
