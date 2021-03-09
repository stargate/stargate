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
package io.stargate.graphql.schema.schemafirst.migration;

import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.schema.Table;

public class CreateTableQuery extends MigrationQuery {

  private final Table table;

  public CreateTableQuery(Table table) {
    this.table = table;
  }

  public Table getTable() {
    return table;
  }

  @Override
  public AbstractBound<?> build(DataStore dataStore) {
    return dataStore
        .queryBuilder()
        .create()
        .table(table.keyspace(), table.name())
        .column(table.columns())
        .build()
        .bind();
  }

  @Override
  public String getDescription() {
    return "Create table " + table.name();
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    // No other migration queries depend on the existence of a table
    return false;
  }

  @Override
  public boolean addsReferenceTo(String udtName) {
    return table.columns().stream().anyMatch(c -> references(c.type(), udtName));
  }

  @Override
  public boolean dropsReferenceTo(String udtName) {
    return false;
  }
}
