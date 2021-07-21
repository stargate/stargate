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
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ParameterizedType;
import io.stargate.db.schema.UserDefinedType;

/** A DDL query to be executed as part of a migration. */
public abstract class MigrationQuery {

  public abstract AbstractBound<?> build(DataStore dataStore);

  public abstract String getDescription();

  public abstract boolean mustRunBefore(MigrationQuery that);

  public abstract void authorize(
      AuthorizationService authorizationService, AuthenticationSubject subject)
      throws UnauthorizedException;

  /** Whether the query will introduce a new column/field that uses the given UDT. */
  protected abstract boolean addsReferenceTo(String udtName);

  /** Whether the query will remove a column/field that was using the given UDT. */
  protected abstract boolean dropsReferenceTo(String udtName);

  protected boolean references(Column.ColumnType type, String udtName) {
    if (type instanceof UserDefinedType) {
      return (type.name().equals(udtName))
          || ((UserDefinedType) type)
              .columns().stream().anyMatch(c -> references(c.type(), udtName));
    } else if (type instanceof ParameterizedType) { // tuples and collections
      return type.parameters().stream().anyMatch(subType -> references(subType, udtName));
    } else { // primitives
      return false;
    }
  }
}
