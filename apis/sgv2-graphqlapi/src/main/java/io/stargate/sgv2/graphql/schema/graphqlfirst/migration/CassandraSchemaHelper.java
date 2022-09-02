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
package io.stargate.sgv2.graphql.schema.graphqlfirst.migration;

import com.google.common.collect.Maps;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.bridge.proto.Schema.ColumnOrderBy;
import io.stargate.bridge.proto.Schema.CqlIndex;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.api.common.cql.builder.Column;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CassandraSchemaHelper {

  /** @return a list of differences, or empty if the tables match. */
  public static List<Difference> compare(CqlTable expectedTable, CqlTable actualTable) {
    String tableName = expectedTable.getName();

    if (!tableName.equals(actualTable.getName())) {
      throw new IllegalArgumentException(
          "This should only be called for tables with the same name");
    }

    List<Difference> differences = new ArrayList<>();

    Map<String, ExtendedColumn> expectedColumns = indexColumns(expectedTable);
    Map<String, ExtendedColumn> actualColumns = indexColumns(actualTable);

    for (Map.Entry<String, ExtendedColumn> entry : expectedColumns.entrySet()) {
      String columnName = entry.getKey();
      ExtendedColumn expectedColumn = entry.getValue();

      ExtendedColumn actualColumn = actualColumns.get(columnName);
      compareColumn(expectedColumn, actualColumn, differences);

      CqlIndex expectedIndex = findSecondaryIndex(expectedTable, columnName);
      if (expectedIndex != null) {
        CqlIndex actualIndex = findSecondaryIndex(actualTable, columnName);
        compareIndex(expectedIndex, actualIndex, expectedColumn, differences);
      }
    }
    return differences;
  }

  private static Map<String, ExtendedColumn> indexColumns(CqlTable table) {
    Map<String, ExtendedColumn> columns =
        Maps.newHashMapWithExpectedSize(
            table.getPartitionKeyColumnsCount()
                + table.getClusteringKeyColumnsCount()
                + table.getColumnsCount()
                + table.getStaticColumnsCount());
    for (ColumnSpec column : table.getPartitionKeyColumnsList()) {
      columns.put(
          column.getName(), new ExtendedColumn(table.getName(), column, Column.Kind.PARTITION_KEY));
    }
    for (ColumnSpec column : table.getClusteringKeyColumnsList()) {
      String name = column.getName();
      columns.put(
          name,
          new ExtendedColumn(
              table.getName(),
              column,
              Column.Kind.CLUSTERING,
              table.getClusteringOrdersMap().get(name)));
    }
    for (ColumnSpec column : table.getColumnsList()) {
      columns.put(
          column.getName(), new ExtendedColumn(table.getName(), column, Column.Kind.REGULAR));
    }
    for (ColumnSpec column : table.getStaticColumnsList()) {
      columns.put(
          column.getName(), new ExtendedColumn(table.getName(), column, Column.Kind.STATIC));
    }
    return columns;
  }

  private static void compareColumn(
      ExtendedColumn expectedColumn, ExtendedColumn actualColumn, List<Difference> differences) {

    TypeSpec expectedType = expectedColumn.spec.getType();

    if (actualColumn == null) {
      String description = null;
      if (expectedColumn.kind == Column.Kind.PARTITION_KEY) {
        description = "it can't be added because it is marked as a partition key";
      } else if (expectedColumn.kind == Column.Kind.CLUSTERING) {
        description = "it can't be added because it is marked as a clustering column";
      }
      differences.add(new Difference(expectedColumn, DifferenceType.MISSING_COLUMN, description));
    } else if (!equals(expectedType, actualColumn.getSpec().getType())) {
      differences.add(
          new Difference(
              expectedColumn,
              DifferenceType.WRONG_TYPE,
              String.format(
                  "expected %s, found %s",
                  TypeSpecs.format(expectedType),
                  TypeSpecs.format(actualColumn.getSpec().getType()))));
    } else if (expectedColumn.kind != actualColumn.kind) {
      differences.add(
          new Difference(
              expectedColumn,
              DifferenceType.WRONG_KIND,
              String.format("expected %s, found %s", expectedColumn.kind, actualColumn.kind)));
    } else if (expectedColumn.getKind() == Column.Kind.CLUSTERING
        && expectedColumn.getOrder() != actualColumn.getOrder()) {
      differences.add(
          new Difference(
              expectedColumn,
              DifferenceType.WRONG_CLUSTERING_ORDER,
              String.format(
                  "expected %s, found %s", expectedColumn.getOrder(), actualColumn.getOrder())));
    }
  }

  /** Compare CQL types, taking into account that UDT references might be shallow. */
  private static boolean equals(TypeSpec expectedType, TypeSpec actualType) {
    if (actualType.getSpecCase() != expectedType.getSpecCase()) {
      return false;
    }
    switch (actualType.getSpecCase()) {
      case BASIC:
        return actualType.getBasic() == expectedType.getBasic();
      case MAP:
        TypeSpec.Map actualMap = actualType.getMap();
        TypeSpec.Map expectedMap = expectedType.getMap();
        return equals(expectedMap.getKey(), actualMap.getKey())
            && equals(expectedMap.getValue(), actualMap.getValue());
      case LIST:
        TypeSpec.List actualList = actualType.getList();
        TypeSpec.List expectedList = expectedType.getList();
        return equals(expectedList.getElement(), actualList.getElement());
      case SET:
        TypeSpec.Set actualSet = actualType.getSet();
        TypeSpec.Set expectedSet = expectedType.getSet();
        return equals(expectedSet.getElement(), actualSet.getElement());
      case UDT:
        return actualType.getUdt().getName().equals(expectedType.getUdt().getName());
      case TUPLE:
        TypeSpec.Tuple actualTuple = actualType.getTuple();
        TypeSpec.Tuple expectedTuple = expectedType.getTuple();
        if (actualTuple.getElementsCount() != expectedTuple.getElementsCount()) {
          return false;
        }
        for (int i = 0; i < actualTuple.getElementsCount(); i++) {
          if (!equals(expectedTuple.getElements(i), actualTuple.getElements(i))) {
            return false;
          }
        }
        return true;
      default:
        throw new AssertionError("Unexpected type " + actualType.getSpecCase());
    }
  }

  private static CqlIndex findSecondaryIndex(CqlTable table, String columnName) {
    for (CqlIndex index : table.getIndexesList()) {
      if (columnName.equals(index.getColumnName())) {
        return index;
      }
    }
    return null;
  }

  private static void compareIndex(
      CqlIndex expectedIndex,
      CqlIndex actualIndex,
      ExtendedColumn expectedColumn,
      List<Difference> differences) {

    if (actualIndex == null) {
      differences.add(
          new Difference(expectedColumn, expectedIndex, DifferenceType.MISSING_INDEX, null));
      return;
    }

    // TODO maybe support dropping/recreating indexes
    String description = null;
    if (!Objects.equals(expectedIndex.getName(), actualIndex.getName())) {
      description =
          String.format(
              "expected name %s, found %s", expectedIndex.getName(), actualIndex.getName());
    } else if (!Objects.equals(expectedIndex.getIndexingClass(), actualIndex.getIndexingClass())) {
      String expectedClass =
          expectedIndex.hasIndexingClass() ? expectedIndex.getIndexingClass().getValue() : "<none>";
      String actualClass =
          actualIndex.hasIndexingClass() ? actualIndex.getIndexingClass().getValue() : "<none>";
      description = String.format("expected index class %s, found %s", expectedClass, actualClass);
    } else if (!Objects.equals(expectedIndex.getIndexingType(), actualIndex.getIndexingType())) {
      description =
          String.format(
              "expected index target %s, found %s",
              expectedIndex.getIndexingType(), actualIndex.getIndexingType());
    } else if (!Objects.equals(expectedIndex.getOptionsMap(), actualIndex.getOptionsMap())) {
      description =
          String.format(
              "expected index options %s, found %s",
              expectedIndex.getOptionsMap(), actualIndex.getOptionsMap());
    }

    if (description != null) {
      differences.add(
          new Difference(expectedColumn, expectedIndex, DifferenceType.WRONG_INDEX, description));
    }
  }

  /** @return a list of differences, or empty if the UDTs match. */
  public static List<Difference> compare(Udt expectedUdt, Udt actualUdt) {
    String typeName = expectedUdt.getName();

    if (!typeName.equals(actualUdt.getName())) {
      throw new IllegalArgumentException("This should only be called for UDTs with the same name");
    }

    List<Difference> differences = new ArrayList<>();
    Map<String, TypeSpec> actualFields = actualUdt.getFieldsMap();
    for (Map.Entry<String, TypeSpec> entry : expectedUdt.getFieldsMap().entrySet()) {
      String fieldName = entry.getKey();
      TypeSpec expectedType = entry.getValue();
      TypeSpec actualType = actualFields.get(fieldName);

      if (actualType == null) {
        String description = null;
        differences.add(
            new Difference(
                toColumn(expectedUdt.getName(), fieldName, expectedType),
                DifferenceType.MISSING_COLUMN,
                description));
      } else if (!equals(expectedType, actualType)) {
        differences.add(
            new Difference(
                toColumn(expectedUdt.getName(), fieldName, expectedType),
                DifferenceType.WRONG_TYPE,
                String.format(
                    "expected %s, found %s",
                    TypeSpecs.format(expectedType), TypeSpecs.format(actualType))));
      }
    }
    return differences;
  }

  private static ExtendedColumn toColumn(String udtName, String fieldName, TypeSpec expectedType) {
    return new ExtendedColumn(
        udtName,
        ColumnSpec.newBuilder().setName(fieldName).setType(expectedType).build(),
        Column.Kind.REGULAR);
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

    private final ExtendedColumn column;
    private final CqlIndex index;
    private final DifferenceType type;
    private final String description;

    public Difference(
        ExtendedColumn column, CqlIndex index, DifferenceType type, String description) {
      this.column = column;
      this.index = index;
      this.type = type;
      this.description = description;
    }

    public Difference(ExtendedColumn column, DifferenceType type, String description) {
      this(column, null, type, description);
    }

    public ExtendedColumn getColumn() {
      return column;
    }

    public CqlIndex getIndex() {
      return index;
    }

    public DifferenceType getType() {
      return type;
    }

    public String toGraphqlMessage() {
      return String.format(
          "[%s] %s.%s%s",
          type,
          column.getTableName(),
          column.getSpec().getName(),
          description == null ? "" : ": " + description);
    }
  }

  public static class ExtendedColumn {
    private final String tableName;
    private final ColumnSpec spec;
    private final Column.Kind kind;
    private final ColumnOrderBy order;

    public ExtendedColumn(
        String tableName, ColumnSpec spec, Column.Kind kind, ColumnOrderBy order) {
      this.tableName = tableName;
      this.spec = spec;
      this.kind = kind;
      this.order = order;
    }

    public ExtendedColumn(String tableName, ColumnSpec spec, Column.Kind kind) {
      this(tableName, spec, kind, null);
    }

    public String getTableName() {
      return tableName;
    }

    public ColumnSpec getSpec() {
      return spec;
    }

    public Column.Kind getKind() {
      return kind;
    }

    public ColumnOrderBy getOrder() {
      return order;
    }
  }

  private CassandraSchemaHelper() {}
}
