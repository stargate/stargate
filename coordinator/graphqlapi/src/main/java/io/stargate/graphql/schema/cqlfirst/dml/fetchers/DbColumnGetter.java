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
package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;

public class DbColumnGetter {
  private final NameMapping nameMapping;

  public DbColumnGetter(NameMapping nameMapping) {
    this.nameMapping = nameMapping;
  }

  public String getDBColumnName(Table table, String fieldName) {
    Column column = getColumn(table, fieldName);
    if (column == null) {
      return null;
    }
    return column.name();
  }

  public Column getColumn(Table table, String fieldName) {
    String columnName = nameMapping.getCqlName(table, fieldName);
    return table.column(columnName);
  }
}
