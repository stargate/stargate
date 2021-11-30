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
import io.stargate.db.Persistence;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.Schema;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.CqlTableCreate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

class CreateTableHandler extends DdlMessageHandler<CqlTableCreate> {

  CreateTableHandler(
      CqlTableCreate createTable,
      Persistence.Connection connection,
      Persistence persistence,
      TypedValue.Codec valueCodec,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<QueryOuterClass.Response> responseObserver) {
    super(
        createTable,
        connection,
        persistence,
        valueCodec,
        executor,
        schemaAgreementRetries,
        responseObserver);
  }

  @Override
  protected void validate() {
    // intentionally empty
  }

  @Override
  protected String buildQuery(
      CqlTableCreate createTableProto, Persistence persistence, QueryBuilder queryBuilder) {
    String keyspaceName =
        persistence.decorateKeyspaceName(
            createTableProto.getKeyspaceName(), GrpcService.HEADERS_KEY.get());
    CqlTable tableProto = createTableProto.getTable();

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
        .ifNotExists(createTableProto.getIfNotExists())
        .column(columns)
        // TODO handle table options
        .build()
        .bind()
        .queryString();
  }
}
