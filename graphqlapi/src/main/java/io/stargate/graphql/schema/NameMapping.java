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
package io.stargate.graphql.schema;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.graphql.util.CaseUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class NameMapping {
  private final BiMap<Table, String> entityName = HashBiMap.create();
  // TODO: using Column as map key is a bit dodgy. First, because Column objects are currently
  //   abused a bit by Result.Rows (in the persistence-api module), which can lead to subtle
  //   issues easily. Second because in general, Column's equality is a tad complex and include
  //   things like the type, which could possibly change over time for a given "column" (in the
  //   sense of "defined in a table"). Using the column name here would be more reliable.
  private final Map<Table, BiMap<Column, String>> columnName;

  public NameMapping(Set<Table> tables) {
    columnName = new HashMap<>();
    buildNames(tables);
  }

  private void buildNames(Set<Table> tables) {
    for (Table table : tables) {
      entityName.put(table, CaseUtil.toCamel(table.name()));
      buildColumnNames(table);
    }
  }

  private void buildColumnNames(Table tableMetadata) {
    BiMap<Column, String> map = columnName.computeIfAbsent(tableMetadata, k -> HashBiMap.create());
    for (Column column : tableMetadata.columns()) {
      map.put(column, CaseUtil.toLowerCamel(column.name()));
    }
  }

  public BiMap<Table, String> getEntityName() {
    return entityName;
  }

  public BiMap<Column, String> getColumnName(Table table) {
    return columnName.get(table);
  }
}
