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
package io.stargate.graphql.schema.cqlfirst.dml;

import com.datastax.oss.driver.shaded.guava.common.collect.BiMap;
import com.datastax.oss.driver.shaded.guava.common.collect.HashBiMap;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.cqlfirst.dml.NameConversions.IdentifierType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameMapping {

  private static final Logger LOG = LoggerFactory.getLogger(NameMapping.class);

  private final BiMap<String, String> entityNames = HashBiMap.create();
  private final Map<String, BiMap<String, String>> columnNames;
  private final BiMap<String, String> udtNames = HashBiMap.create();
  private final Map<String, BiMap<String, String>> fieldNames;
  private final List<String> warnings;

  public NameMapping(Set<Table> tables, List<UserDefinedType> udts, List<String> warnings) {
    this.warnings = warnings;
    columnNames = new HashMap<>();
    buildNames(tables);
    fieldNames = new HashMap<>();
    buildNames(udts);
  }

  private void buildNames(Set<Table> tables) {
    for (Table table : tables) {
      String graphqlName = NameConversions.toGraphql(table.name(), IdentifierType.TABLE);
      String clashingCqlName = entityNames.inverse().get(graphqlName);
      if (clashingCqlName != null) {
        String message =
            String.format(
                "Couldn't convert table %s because its GraphQL name %s would collide with table %s",
                table.name(), graphqlName, clashingCqlName);
        warnings.add(message);
        // Also log because this is a bug, it means there is a hole in our naming rules
        LOG.warn(message);
      } else {
        if (!graphqlName.equals(table.name())) {
          warnings.add(String.format("Table %s mapped as %s", table.name(), graphqlName));
        }
        entityNames.put(table.name(), graphqlName);
        columnNames.put(table.name(), buildColumnNames(table.columns()));
      }
    }
  }

  private void buildNames(List<UserDefinedType> udts) {
    for (UserDefinedType udt : udts) {
      String graphqlName = NameConversions.toGraphql(udt.name(), IdentifierType.UDT);
      String clashingCqlName = udtNames.inverse().get(graphqlName);
      if (clashingCqlName != null) {
        String message =
            String.format(
                "Could not convert UDT %s because its GraphQL name %s would collide with UDT %s",
                udt.name(), graphqlName, clashingCqlName);
        warnings.add(message);
        LOG.warn(message);
      } else {
        if (!graphqlName.equals(udt.name() + "Udt")) {
          warnings.add(String.format("UDT %s mapped as %s", udt.name(), graphqlName));
        }
        udtNames.put(udt.name(), graphqlName);
        fieldNames.put(udt.name(), buildColumnNames(udt.columns()));
      }
    }
  }

  private BiMap<String, String> buildColumnNames(List<Column> columns) {
    BiMap<String, String> map = HashBiMap.create();
    for (Column column : columns) {
      String graphqlName = NameConversions.toGraphql(column.name(), IdentifierType.COLUMN);
      String clashingCqlName = map.inverse().get(graphqlName);
      if (clashingCqlName != null) {
        String message =
            String.format(
                "Could not convert column %s in table/UDT %s because its GraphQL name %s "
                    + "would collide with column %s",
                column.name(), column.table(), graphqlName, clashingCqlName);
        warnings.add(message);
        LOG.warn(message);
      } else {
        if (!graphqlName.equals(column.name())) {
          warnings.add(
              String.format(
                  "Column %s in table/UDT %s mapped as %s",
                  column.name(), column.table(), graphqlName));
        }
        map.put(column.name(), graphqlName);
      }
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
