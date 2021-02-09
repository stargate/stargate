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
import io.stargate.db.schema.UserDefinedType;

public class AddUdtFieldQuery extends MigrationQuery {

  private final UserDefinedType type;
  private final Column column;

  public AddUdtFieldQuery(UserDefinedType type, Column column) {
    this.type = type;
    this.column = column;
  }

  @Override
  public AbstractBound<?> build(DataStore dataStore) {
    return dataStore
        .queryBuilder()
        .alter()
        .type(type.keyspace(), type)
        .addColumn(column)
        .build()
        .bind();
  }

  @Override
  public String getDescription() {
    return String.format("Add field %s to UDT %s", column.name(), type.name());
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
