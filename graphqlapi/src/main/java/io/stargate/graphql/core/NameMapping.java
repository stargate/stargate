package io.stargate.graphql.core;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.Table;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class NameMapping {
  private final BiMap<Table, String> entityName = HashBiMap.create();
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
    if (columnName.get(tableMetadata) == null) {
      columnName.put(tableMetadata, HashBiMap.create());
    }

    BiMap map = columnName.get(tableMetadata);
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
