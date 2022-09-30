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

import io.stargate.db.schema.Column;
import io.stargate.db.schema.SecondaryIndex;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.graphqlfirst.processor.IndexTarget;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CassandraSchemaHelper {

  /** @return a list of differences, or empty if the tables match. */
  public static List<Difference> compare(Table expectedTable, Table actualTable) {
    String tableName = expectedTable.name();

    if (!tableName.equals(actualTable.name())) {
      throw new IllegalArgumentException(
          "This should only be called for tables with the same name");
    }

    List<Difference> differences = new ArrayList<>();

    for (Column expectedColumn : expectedTable.columns()) {
      Column actualColumn = actualTable.column(expectedColumn.name());
      compareColumn(expectedColumn, actualColumn, differences);

      SecondaryIndex expectedIndex = findSecondaryIndex(expectedTable, expectedColumn);
      if (expectedIndex != null) {
        SecondaryIndex actualIndex =
            (actualColumn == null) ? null : findSecondaryIndex(actualTable, actualColumn);
        compareIndex(expectedIndex, actualIndex, expectedColumn, differences);
      }
    }
    return differences;
  }

  private static void compareColumn(
      Column expectedColumn, Column actualColumn, List<Difference> differences) {

    Column.ColumnType expectedType = expectedColumn.type();
    assert expectedType != null;

    if (actualColumn == null) {
      String description = null;
      if (expectedColumn.isPartitionKey()) {
        description = "it can't be added because it is marked as a partition key";
      } else if (expectedColumn.isClusteringKey()) {
        description = "it can't be added because it is marked as a clustering column";
      }
      differences.add(new Difference(expectedColumn, DifferenceType.MISSING_COLUMN, description));
    } else if (!equals(expectedType, actualColumn.type())) {
      differences.add(
          new Difference(
              expectedColumn,
              DifferenceType.WRONG_TYPE,
              String.format(
                  "expected %s, found %s",
                  expectedType.cqlDefinition(), actualColumn.type().cqlDefinition())));
    } else if (expectedColumn.kind() != actualColumn.kind()) {
      differences.add(
          new Difference(
              expectedColumn,
              DifferenceType.WRONG_KIND,
              String.format("expected %s, found %s", expectedColumn.kind(), actualColumn.kind())));
    } else if (expectedColumn.kind() == Column.Kind.Clustering
        && expectedColumn.order() != actualColumn.order()) {
      differences.add(
          new Difference(
              expectedColumn,
              DifferenceType.WRONG_CLUSTERING_ORDER,
              String.format(
                  "expected %s, found %s", expectedColumn.order(), actualColumn.order())));
    }
  }

  /** Compare CQL types, taking into account that UDT references might be shallow. */
  private static boolean equals(Column.ColumnType expectedType, Column.ColumnType actualType) {
    if (actualType.rawType() != expectedType.rawType()) {
      return false;
    }
    if (actualType.isUserDefined()) {
      return actualType.name().equals(expectedType.name());
    }
    if (actualType.isParameterized()) {
      List<Column.ColumnType> actualParameters = actualType.parameters();
      List<Column.ColumnType> expectedParameters = expectedType.parameters();
      if (actualParameters.size() != expectedParameters.size()) {
        return false;
      }
      for (int i = 0; i < actualParameters.size(); i++) {
        if (!equals(expectedParameters.get(i), actualParameters.get(i))) {
          return false;
        }
      }
      return true;
    }
    // Else it's a primitive type, and we've already compared the raw type
    return true;
  }

  private static SecondaryIndex findSecondaryIndex(Table table, Column column) {
    if (column == null) {
      return null;
    }
    return (SecondaryIndex)
        table.indexes().stream()
            .filter(
                index ->
                    index instanceof SecondaryIndex
                        && column.equals(((SecondaryIndex) index).column()))
            .findFirst()
            .orElse(null);
  }

  private static void compareIndex(
      SecondaryIndex expectedIndex,
      SecondaryIndex actualIndex,
      Column expectedColumn,
      List<Difference> differences) {

    if (actualIndex == null) {
      differences.add(
          new Difference(expectedColumn, expectedIndex, DifferenceType.MISSING_INDEX, null));
      return;
    }

    // TODO maybe support dropping/recreating indexes
    String description = null;
    if (!Objects.equals(expectedIndex.name(), actualIndex.name())) {
      description =
          String.format("expected name %s, found %s", expectedIndex.name(), actualIndex.name());
    } else if (!Objects.equals(expectedIndex.indexingClass(), actualIndex.indexingClass())) {
      description =
          String.format(
              "expected index class %s, found %s",
              expectedIndex.indexingClass(), actualIndex.indexingClass());
    } else if (!Objects.equals(expectedIndex.indexingType(), actualIndex.indexingType())) {
      description =
          String.format(
              "expected index target %s, found %s",
              IndexTarget.fromIndexingType(expectedIndex.indexingType()),
              IndexTarget.fromIndexingType(actualIndex.indexingType()));
    } else if (!Objects.equals(expectedIndex.indexingOptions(), actualIndex.indexingOptions())) {
      description =
          String.format(
              "expected index options %s, found %s",
              expectedIndex.indexingOptions(), actualIndex.indexingOptions());
    }

    if (description != null) {
      differences.add(
          new Difference(expectedColumn, expectedIndex, DifferenceType.WRONG_INDEX, description));
    }
  }

  /** @return a list of differences, or empty if the UDTs match. */
  public static List<Difference> compare(UserDefinedType expectedType, UserDefinedType actualType) {
    String typeName = expectedType.name();

    if (!typeName.equals(actualType.name())) {
      throw new IllegalArgumentException("This should only be called for UDTs with the same name");
    }

    List<Difference> differences = new ArrayList<>();
    for (Column expectedColumn : expectedType.columns()) {
      Column actualColumn = actualType.columnMap().get(expectedColumn.name());
      compareColumn(expectedColumn, actualColumn, differences);
    }
    return differences;
  }

  public enum DifferenceType {
    MISSING_COLUMN,
    WRONG_TYPE,
    WRONG_KIND,
    WRONG_CLUSTERING_ORDER,
    MISSING_INDEX,
    WRONG_INDEX,
  }

  public static class Difference {

    private final Column column;
    private final SecondaryIndex index;
    private final DifferenceType type;
    private final String description;

    public Difference(
        Column column, SecondaryIndex index, DifferenceType type, String description) {
      this.column = column;
      this.index = index;
      this.type = type;
      this.description = description;
    }

    public Difference(Column column, DifferenceType type, String description) {
      this(column, null, type, description);
    }

    public Column getColumn() {
      return column;
    }

    public SecondaryIndex getIndex() {
      return index;
    }

    public DifferenceType getType() {
      return type;
    }

    public String toGraphqlMessage() {
      return String.format(
          "[%s] %s.%s%s",
          type, column.table(), column.name(), description == null ? "" : ": " + description);
    }
  }

  private CassandraSchemaHelper() {}
}
