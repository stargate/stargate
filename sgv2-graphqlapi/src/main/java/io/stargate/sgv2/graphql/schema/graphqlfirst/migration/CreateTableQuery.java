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
package io.stargate.sgv2.graphql.schema.graphqlfirst.migration;

import com.google.common.collect.ImmutableList;
import io.stargate.grpc.TypeSpecs;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.Schema;
import io.stargate.proto.Schema.CqlIndex;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import java.util.List;

public class CreateTableQuery extends MigrationQuery {

  private final CqlTable table;

  public static List<MigrationQuery> createTableAndIndexes(String keyspaceName, CqlTable table) {
    ImmutableList.Builder<MigrationQuery> builder = ImmutableList.builder();
    builder.add(new CreateTableQuery(keyspaceName, table));
    for (CqlIndex index : table.getIndexesList()) {
      builder.add(new CreateIndexQuery(keyspaceName, table.getName(), index));
    }
    return builder.build();
  }

  public CreateTableQuery(String keyspaceName, CqlTable table) {
    super(keyspaceName);
    this.table = table;
  }

  public String getTableName() {
    return table.getName();
  }

  @Override
  public Query build() {
    return new QueryBuilder()
        .create()
        .table(keyspaceName, table.getName())
        .column(buildColumns())
        .build();
  }

  private List<Column> buildColumns() {
    ImmutableList.Builder<Column> columns = ImmutableList.builder();
    for (ColumnSpec column : table.getPartitionKeyColumnsList()) {
      columns.add(
          ImmutableColumn.builder()
              .name(column.getName())
              .type(TypeSpecs.format(column.getType()))
              .kind(Column.Kind.PARTITION_KEY)
              .build());
    }
    for (ColumnSpec column : table.getClusteringKeyColumnsList()) {
      Schema.ColumnOrderBy order = table.getClusteringOrdersMap().get(column.getName());
      columns.add(
          ImmutableColumn.builder()
              .name(column.getName())
              .type(TypeSpecs.format(column.getType()))
              .kind(Column.Kind.CLUSTERING)
              .order(order == Schema.ColumnOrderBy.ASC ? Column.Order.ASC : Column.Order.DESC)
              .build());
    }
    for (ColumnSpec column : table.getColumnsList()) {
      columns.add(
          ImmutableColumn.builder()
              .name(column.getName())
              .type(TypeSpecs.format(column.getType()))
              .kind(Column.Kind.REGULAR)
              .build());
    }
    for (ColumnSpec column : table.getStaticColumnsList()) {
      columns.add(
          ImmutableColumn.builder()
              .name(column.getName())
              .type(TypeSpecs.format(column.getType()))
              .kind(Column.Kind.STATIC)
              .build());
    }
    return columns.build();
  }

  @Override
  public String getDescription() {
    return "Create table " + table.getName();
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    // Must create a table before creating any index on it.
    return (that instanceof CreateIndexQuery)
        && ((CreateIndexQuery) that).getTableName().equals(getTableName());
  }

  @Override
  public boolean addsReferenceTo(String udtName) {
    return references(udtName, this.table);
  }

  @Override
  public boolean dropsReferenceTo(String udtName) {
    return false;
  }
}
