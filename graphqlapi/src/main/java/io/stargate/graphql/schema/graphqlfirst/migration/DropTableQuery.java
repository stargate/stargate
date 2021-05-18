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
package io.stargate.graphql.schema.graphqlfirst.migration;

import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.schema.Table;

public class DropTableQuery extends MigrationQuery {

  private final Table table;

  public DropTableQuery(Table table) {
    this.table = table;
  }

  public Table getTable() {
    return table;
  }

  @Override
  public AbstractBound<?> build(DataStore dataStore) {
    return dataStore
        .queryBuilder()
        .drop()
        .table(table.keyspace(), table.name())
        .ifExists()
        .build()
        .bind();
  }

  @Override
  public String getDescription() {
    return "Drop table " + table.name();
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    // Must keep "drop and recreate" operations in the correct order
    if (that instanceof CreateTableQuery) {
      return table.name().equals(((CreateTableQuery) that).getTable().name());
    }
    // Must drop all references to a UDT before it gets dropped
    if (that instanceof DropUdtQuery) {
      return this.dropsReferenceTo(((DropUdtQuery) that).getType().name());
    }
    return false;
  }

  @Override
  public void authorize(AuthorizationService authorizationService, AuthenticationSubject subject)
      throws UnauthorizedException {
    authorizationService.authorizeSchemaWrite(
        subject, table.keyspace(), table.name(), Scope.DROP, SourceAPI.GRAPHQL, ResourceKind.TABLE);
  }

  @Override
  public boolean addsReferenceTo(String udtName) {
    return false;
  }

  @Override
  public boolean dropsReferenceTo(String udtName) {
    return table.columns().stream().anyMatch(c -> references(c.type(), udtName));
  }
}
