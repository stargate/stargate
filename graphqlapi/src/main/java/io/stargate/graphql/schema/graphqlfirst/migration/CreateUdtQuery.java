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
import io.stargate.db.schema.UserDefinedType;
import java.util.Collections;
import java.util.List;

public class CreateUdtQuery extends MigrationQuery {

  private final UserDefinedType type;

  /**
   * This method only exist to mirror {@link CreateTableQuery#createTableAndIndexes} (there is some
   * common code that abstracts over both).
   */
  public static List<MigrationQuery> createUdt(UserDefinedType type) {
    return Collections.singletonList(new CreateUdtQuery(type));
  }

  public CreateUdtQuery(UserDefinedType type) {
    this.type = type;
  }

  public UserDefinedType getType() {
    return type;
  }

  @Override
  public AbstractBound<?> build(DataStore dataStore) {
    return dataStore.queryBuilder().create().type(type.keyspace(), type).build().bind();
  }

  @Override
  public String getDescription() {
    return "Create UDT " + type.name();
  }

  @Override
  public void authorize(AuthorizationService authorizationService, AuthenticationSubject subject)
      throws UnauthorizedException {
    authorizationService.authorizeSchemaWrite(
        subject, type.keyspace(), null, Scope.CREATE, SourceAPI.GRAPHQL, ResourceKind.TYPE);
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    // Must create a UDT before it gets referenced
    return that.addsReferenceTo(type.name());
  }

  @Override
  public boolean addsReferenceTo(String udtName) {
    return type.columns().stream().anyMatch(c -> references(c.type(), udtName));
  }

  @Override
  public boolean dropsReferenceTo(String udtName) {
    return false;
  }
}
