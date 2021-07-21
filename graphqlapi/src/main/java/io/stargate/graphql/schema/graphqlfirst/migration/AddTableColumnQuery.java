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
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;

public class AddTableColumnQuery extends MigrationQuery {

  private final Table table;
  private final Column column;

  public AddTableColumnQuery(Table table, Column column) {
    this.table = table;
    this.column = column;
  }

  @Override
  public AbstractBound<?> build(DataStore dataStore) {
    return dataStore
        .queryBuilder()
        .alter()
        .table(table.keyspace(), table.name())
        .addColumn(column)
        .build()
        .bind();
  }

  @Override
  public String getDescription() {
    return String.format("Add column %s to table %s", column.name(), table.name());
  }

  @Override
  public void authorize(AuthorizationService authorizationService, AuthenticationSubject subject)
      throws UnauthorizedException {
    authorizationService.authorizeSchemaWrite(
        subject,
        table.keyspace(),
        table.name(),
        Scope.ALTER,
        SourceAPI.GRAPHQL,
        ResourceKind.TABLE);
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
    return references(column.type(), udtName);
  }

  @Override
  public boolean dropsReferenceTo(String udtName) {
    return false;
  }
}
