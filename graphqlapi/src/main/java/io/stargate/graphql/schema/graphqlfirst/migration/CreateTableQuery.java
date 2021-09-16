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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.SecondaryIndex;
import io.stargate.db.schema.Table;
import java.util.List;

public class CreateTableQuery extends MigrationQuery {

  private final Table table;

  public static List<MigrationQuery> createTableAndIndexes(Table table) {
    ImmutableList.Builder<MigrationQuery> builder = ImmutableList.builder();
    builder.add(new CreateTableQuery(table));
    for (Index index : table.indexes()) {
      if (index instanceof SecondaryIndex) {
        builder.add(new CreateIndexQuery(table, ((SecondaryIndex) index)));
      }
    }
    return builder.build();
  }

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
  public void authorize(AuthorizationService authorizationService, AuthenticationSubject subject)
      throws UnauthorizedException {
    authorizationService.authorizeSchemaWrite(
        subject,
        table.keyspace(),
        table.name(),
        Scope.CREATE,
        SourceAPI.GRAPHQL,
        ResourceKind.TABLE);
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    // Must create a table before creating any index on it.
    return (that instanceof CreateIndexQuery)
        && ((CreateIndexQuery) that).getTable().name().equals(table.name());
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
