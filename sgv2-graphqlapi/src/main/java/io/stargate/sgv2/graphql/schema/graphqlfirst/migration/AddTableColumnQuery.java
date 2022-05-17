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

import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.graphql.schema.graphqlfirst.migration.CassandraSchemaHelper.ExtendedColumn;

public class AddTableColumnQuery extends MigrationQuery {

  private final String tableName;
  private final ExtendedColumn column;

  public AddTableColumnQuery(String keyspaceName, String tableName, ExtendedColumn column) {
    super(keyspaceName);
    this.tableName = tableName;
    this.column = column;
  }

  @Override
  public Query build() {
    return new QueryBuilder()
        .alter()
        .table(keyspaceName, tableName)
        .addColumn(column.getSpec().getName(), TypeSpecs.format(column.getSpec().getType()))
        .build();
  }

  @Override
  public String getDescription() {
    return String.format("Add column %s to table %s", column.getSpec().getName(), tableName);
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    // We want all column additions as early as possible. This is because these queries can fail
    // unexpectedly if the column previously existed with a different type, and we have no way to
    // check that beforehand. If this happens, we want to execute as few queries as possible before
    // we find out.

    // Avoid an infinite loop
    if (that instanceof AddTableColumnQuery) {
      return false;
    }

    // Otherwise, unless there is already an ordering, move this one first
    return !that.mustRunBefore(this);
  }

  @Override
  public boolean addsReferenceTo(String udtName) {
    return references(column.getSpec().getType(), udtName);
  }

  @Override
  public boolean dropsReferenceTo(String udtName) {
    return false;
  }
}
