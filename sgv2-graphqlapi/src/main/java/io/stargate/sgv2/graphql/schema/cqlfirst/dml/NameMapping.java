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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameConversions.IdentifierType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameMapping {

  private static final Logger LOG = LoggerFactory.getLogger(NameMapping.class);

  private final BiMap<String, String> entityNames = HashBiMap.create();
  private final Map<String, BiMap<String, String>> columnNames;
  private final BiMap<String, String> udtNames = HashBiMap.create();
  private final Map<String, BiMap<String, String>> fieldNames;
  private final List<String> warnings;

  public NameMapping(List<CqlTable> tables, List<Udt> udts, List<String> warnings) {
    this.warnings = warnings;
    columnNames = new HashMap<>();
    buildTableNames(tables);
    fieldNames = new HashMap<>();
    buildUdtNames(udts);
  }

  private void buildTableNames(List<CqlTable> tables) {
    for (CqlTable table : tables) {
      String cqlName = table.getName();
      String graphqlName = NameConversions.toGraphql(cqlName, IdentifierType.TABLE);
      String clashingCqlName = entityNames.inverse().get(graphqlName);
      if (clashingCqlName != null) {
        String message =
            String.format(
                "Couldn't convert table %s because its GraphQL name %s would collide with table %s",
                cqlName, graphqlName, clashingCqlName);
        warnings.add(message);
        // Also log because this is a bug, it means there is a hole in our naming rules
        LOG.warn(message);
      } else {
        if (!graphqlName.equals(cqlName)) {
          warnings.add(String.format("Table %s mapped as %s", cqlName, graphqlName));
        }
        entityNames.put(cqlName, graphqlName);
        columnNames.put(cqlName, buildColumnNames(table));
      }
    }
  }

  private void buildUdtNames(List<Udt> udts) {
    for (Udt udt : udts) {
      String cqlName = udt.getName();
      String graphqlName = NameConversions.toGraphql(cqlName, IdentifierType.UDT);
      String clashingCqlName = udtNames.inverse().get(graphqlName);
      if (clashingCqlName != null) {
        String message =
            String.format(
                "Could not convert UDT %s because its GraphQL name %s would collide with UDT %s",
                cqlName, graphqlName, clashingCqlName);
        warnings.add(message);
        LOG.warn(message);
      } else {
        if (!graphqlName.equals(cqlName + "Udt")) {
          warnings.add(String.format("UDT %s mapped as %s", cqlName, graphqlName));
        }
        udtNames.put(cqlName, graphqlName);
        fieldNames.put(cqlName, buildFieldNames(udt));
      }
    }
  }

  private BiMap<String, String> buildColumnNames(CqlTable table) {
    String tableName = table.getName();
    BiMap<String, String> map = HashBiMap.create();
    buildColumnNames(
        table.getPartitionKeyColumnsList().stream().map(ColumnSpec::getName), map, tableName);
    buildColumnNames(
        table.getClusteringKeyColumnsList().stream().map(ColumnSpec::getName), map, tableName);
    buildColumnNames(table.getColumnsList().stream().map(ColumnSpec::getName), map, tableName);
    buildColumnNames(
        table.getStaticColumnsList().stream().map(ColumnSpec::getName), map, tableName);
    return map;
  }

  private BiMap<String, String> buildFieldNames(Udt udt) {
    String udtName = udt.getName();
    BiMap<String, String> map = HashBiMap.create();
    buildColumnNames(udt.getFieldsMap().keySet().stream(), map, udtName);
    return map;
  }

  private void buildColumnNames(
      Stream<String> columnNames, BiMap<String, String> map, String tableName) {
    columnNames.forEach(
        columnName -> {
          String graphqlName = NameConversions.toGraphql(columnName, IdentifierType.COLUMN);
          String clashingCqlName = map.inverse().get(graphqlName);
          if (clashingCqlName != null) {
            String message =
                String.format(
                    "Could not convert column %s in table/UDT %s because its GraphQL name %s "
                        + "would collide with column %s",
                    columnName, tableName, graphqlName, clashingCqlName);
            warnings.add(message);
            LOG.warn(message);
          } else {
            if (!graphqlName.equals(columnName)) {
              warnings.add(
                  String.format(
                      "Column %s in table/UDT %s mapped as %s",
                      columnName, tableName, graphqlName));
            }
            map.put(columnName, graphqlName);
          }
        });
  }

  public String getGraphqlName(CqlTable table) {
    return entityNames.get(table.getName());
  }

  public String getGraphqlName(CqlTable table, ColumnSpec column) {
    return columnNames.get(table.getName()).get(column.getName());
  }

  public String getCqlName(CqlTable table, String graphqlName) {
    return columnNames.get(table.getName()).inverse().get(graphqlName);
  }

  public String getGraphqlName(Udt udt) {
    return udtNames.get(udt.getName());
  }

  public String getGraphqlName(Udt udt, String fieldName) {
    return fieldNames.get(udt.getName()).get(fieldName);
  }

  public String getCqlName(Udt udt, String graphqlName) {
    return fieldNames.get(udt.getName()).inverse().get(graphqlName);
  }
}
