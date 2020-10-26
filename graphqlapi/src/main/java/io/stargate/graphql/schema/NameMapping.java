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
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.util.CaseUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NameMapping {
  private final BiMap<String, String> entityNames = HashBiMap.create();
  private final Map<String, BiMap<String, String>> columnNames;
  private final BiMap<String, String> udtNames = HashBiMap.create();
  private final Map<String, BiMap<String, String>> fieldNames;

  public NameMapping(Set<Table> tables, List<UserDefinedType> udts) {
    columnNames = new HashMap<>();
    buildNames(tables);
    fieldNames = new HashMap<>();
    buildNames(udts);
  }

  private void buildNames(Set<Table> tables) {
    for (Table table : tables) {
      entityNames.put(table.name(), CaseUtil.toCamel(table.name()));
      columnNames.put(table.name(), buildColumnNames(table.columns()));
    }
  }

  private void buildNames(List<UserDefinedType> udts) {
    for (UserDefinedType udt : udts) {
      // CQL allows tables and UDTs with the same name, append a suffix to avoid clashes.
      udtNames.put(udt.name(), CaseUtil.toCamel(udt.name()) + "Udt");
      fieldNames.put(udt.name(), buildColumnNames(udt.columns()));
    }
  }

  private BiMap<String, String> buildColumnNames(List<Column> columns) {
    BiMap<String, String> map = HashBiMap.create();
    for (Column column : columns) {
      map.put(column.name(), CaseUtil.toLowerCamel(column.name()));
    }
    return map;
  }

  public String getGraphqlName(Table table) {
    return entityNames.get(table.name());
  }

  public String getGraphqlName(Table table, Column column) {
    return columnNames.get(table.name()).get(column.name());
  }

  public String getCqlName(Table table, String graphqlName) {
    return columnNames.get(table.name()).inverse().get(graphqlName);
  }

  public String getGraphqlName(UserDefinedType udt) {
    return udtNames.get(udt.name());
  }

  public String getGraphqlName(UserDefinedType udt, Column column) {
    return fieldNames.get(udt.name()).get(column.name());
  }

  public String getCqlName(UserDefinedType udt, String graphqlName) {
    return fieldNames.get(udt.name()).inverse().get(graphqlName);
  }
}
