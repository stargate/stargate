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

import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;

public class DropTableQuery extends MigrationQuery {

  private final CqlTable table;

  public DropTableQuery(String keyspaceName, CqlTable table) {
    super(keyspaceName);
    this.table = table;
  }

  public String getTableName() {
    return table.getName();
  }

  @Override
  public Query build() {
    return new QueryBuilder().drop().table(keyspaceName, table.getName()).ifExists().build();
  }

  @Override
  public String getDescription() {
    return "Drop table " + table.getName();
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    // Must keep "drop and recreate" operations in the correct order
    if (that instanceof CreateTableQuery) {
      return table.getName().equals(((CreateTableQuery) that).getTableName());
    }
    // Must drop all references to a UDT before it gets dropped
    if (that instanceof DropUdtQuery) {
      return this.dropsReferenceTo(((DropUdtQuery) that).getTypeName());
    }
    return false;
  }

  @Override
  public boolean addsReferenceTo(String udtName) {
    return false;
  }

  @Override
  public boolean dropsReferenceTo(String udtName) {
    return references(udtName, table);
  }
}
