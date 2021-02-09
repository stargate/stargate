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
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ParameterizedType;
import io.stargate.db.schema.UserDefinedType;

/** A DDL query to be executed as part of a migration. */
public abstract class MigrationQuery {

  public abstract AbstractBound<?> build(DataStore dataStore);

  public abstract String getDescription();

  public boolean mustRunBefore(MigrationQuery that) {
    if (this instanceof CreateUdtQuery) {
      // A UDT must exist before we introduce a reference to it.
      return that.addsReferenceTo(((CreateUdtQuery) this).getType().name());
    } else if (that instanceof DropUdtQuery) {
      // All references to a UDT must be dropped before we drop it.
      return this.dropsReferenceTo(((DropUdtQuery) that).getType().name());
    } else {
      // In addition, we move all column additions as close to the beginning as possible. This is
      // because these queries can fail unexpectedly if the column previously existed with a
      // different type, and we have no way to check that beforehand. If this happens, we want to
      // execute as few queries as possible before we find out.
      return this instanceof AddTableColumnQuery
          && !(that instanceof AddTableColumnQuery)
          && !that.mustRunBefore(this);
    }
  }

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
