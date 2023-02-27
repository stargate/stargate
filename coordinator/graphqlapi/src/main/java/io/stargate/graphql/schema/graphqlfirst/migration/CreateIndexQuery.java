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
import io.stargate.db.schema.SecondaryIndex;
import io.stargate.db.schema.Table;

public class CreateIndexQuery extends MigrationQuery {

  private final Table table;
  private final SecondaryIndex index;

  public CreateIndexQuery(Table table, SecondaryIndex index) {
    this.table = table;
    this.index = index;
  }

  public Table getTable() {
    return table;
  }

  @Override
  public AbstractBound<?> build(DataStore dataStore) {
    return dataStore
        .queryBuilder()
        .create()
        .index(index.name())
        .on(table)
        .column(index.column())
        .indexingType(index.indexingType())
        .custom(index.indexingClass())
        .options(index.indexingOptions())
        .build()
        .bind();
  }

  @Override
  public String getDescription() {
    return String.format(
        "Create index %s on %s.%s", index.name(), table.name(), index.column().name());
  }

  @Override
  public void authorize(AuthorizationService authorizationService, AuthenticationSubject subject)
      throws UnauthorizedException {
    authorizationService.authorizeSchemaWrite(
        subject,
        table.keyspace(),
        table.name(),
        Scope.CREATE,
        SourceAPI.GRAPHQL,
        ResourceKind.INDEX);
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    return false;
  }

  @Override
  protected boolean addsReferenceTo(String udtName) {
    return false;
  }

  @Override
  protected boolean dropsReferenceTo(String udtName) {
    return false;
  }
}
