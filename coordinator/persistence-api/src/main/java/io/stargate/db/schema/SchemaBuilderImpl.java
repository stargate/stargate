/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.db.schema;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Convenience builder for creating schema. */
public class SchemaBuilderImpl {
  private Set<Keyspace> keyspaces = new LinkedHashSet<>();
  private Set<Table> tables = new LinkedHashSet<>();
  private Set<Column> columns = new LinkedHashSet<>();
  private Set<Column> materializedViewColumns = new LinkedHashSet<>();
  private Set<Index> indexes = new LinkedHashSet<>();
  private String keyspaceName;
  private String tableName;
  private String materializedViewName;
  private Column secondaryIndexColumn;
  private String secondaryIndexName;
  private Optional<Consumer<Schema>> callback;
  private String udtTypeName;
  private Set<UserDefinedType> udts = new LinkedHashSet<>();
  private boolean indexKeys;
  private boolean indexValues;
  private boolean indexEntries;
  private boolean indexFull;
  private String indexClass;
  private Map<String, String> indexOptions = new HashMap<>();
  private List<Column> fromColumns = new ArrayList<>();
  private List<Column> toColumns = new ArrayList<>();
  private Map<String, String> replication = Collections.emptyMap();
  private Optional<Boolean> durableWrites = Optional.empty();

  public SchemaBuilderImpl(Optional<Consumer<Schema>> callback) {
    this.callback = callback;
  }

  public SchemaBuilderImpl keyspace(String name) {
    if (keyspaceName != null) {
      table(null);

      keyspaces.add(Keyspace.create(keyspaceName, tables, udts, replication, durableWrites));
      tables.clear();
      udts.clear();

      replication = Collections.emptyMap();
      durableWrites = Optional.empty();
    }
    keyspaceName = name;
    return this;
  }

  public SchemaBuilderImpl type(String typeName) {
    finishLast();
    this.udtTypeName = typeName;
    return this;
  }

  private void finishLastUDT() {
    if (null != udtTypeName) {
      Set<Column> cols = new LinkedHashSet<>(columns.size());
      columns.stream().forEach(c -> cols.add(maybeFreezeIfComplexSubtype(c)));
      udts.add(
          ImmutableUserDefinedType.builder()
              .keyspace(keyspaceName)
              .name(udtTypeName)
              .columns(cols)
              .build()
              .dereference(udtKeyspace()));
      columns.clear();
      this.udtTypeName = null;
    }
  }

  private Column maybeFreezeIfComplexSubtype(Column column) {
    if (column.type().isComplexType() && !column.type().isFrozen()) {
      return ImmutableColumn.builder().from(column).type(column.type().frozen()).build();
    }
    return column;
  }

  private ImmutableKeyspace udtKeyspace() {
    return ImmutableKeyspace.builder().name(keyspaceName).userDefinedTypes(udts).build();
  }

  public SchemaBuilderImpl table(String name) {
    finishLast();
    if (tableName != null) {
      Table table =
          Table.create(
              keyspaceName,
              tableName,
              ImmutableList.copyOf(columns),
              ImmutableList.copyOf(indexes));
      Preconditions.checkArgument(
          !table.primaryKeyColumns().isEmpty(),
          "Table '%s' must have at least one primary key column",
          table.name());
      tables.add(table);
      indexes.clear();
      columns.clear();
    }
    tableName = name;
    return this;
  }

  private boolean finishingLast = false;

  public void finishLast() {
    if (!finishingLast) {
      try {
        finishingLast = true;
        finishLastUDT();
        finishLastSecondaryIndex();
        finishLastMaterializedView();
      } finally {
        finishingLast = false;
      }
    }
  }

  public SchemaBuilderImpl column(String name, Column.ColumnType type, Column.Kind kind) {
    return column(name, type, kind, kind == Column.Kind.Clustering ? Column.Order.ASC : null);
  }

  public SchemaBuilderImpl column(
      String name, Column.ColumnType type, Column.Kind kind, Column.Order order) {
    checkIndexOnColumn(name);
    checkMvOnColumn(name);

    columns.add(
        ImmutableColumn.builder()
            .keyspace(keyspaceName)
            .table(tableName)
            .name(name)
            .type(type.dereference(udtKeyspace()))
            .kind(kind)
            .order(order)
            .build());
    return this;
  }

  public SchemaBuilderImpl column(String name, Class type, Column.Kind kind) {
    return column(name, type, kind, kind == Column.Kind.Clustering ? Column.Order.ASC : null);
  }

  public SchemaBuilderImpl column(String name, Class type, Column.Kind kind, Column.Order order) {
    checkIndexOnColumn(name);
    checkMvOnColumn(name);

    columns.add(
        ImmutableColumn.builder()
            .keyspace(keyspaceName)
            .table(tableName)
            .name(name)
            .type(type)
            .kind(kind)
            .order(order)
            .build());
    return this;
  }

  public SchemaBuilderImpl column(String name, Column.Kind kind) {
    checkIndexOnColumn(name);

    Preconditions.checkState(
        materializedViewName != null,
        "This overload of 'column' can only be used with materialized views");
    Column.Order order = kind == Column.Kind.Clustering ? Column.Order.ASC : null;
    materializedViewColumns.add(
        ImmutableColumn.builder()
            .keyspace(keyspaceName)
            .table(tableName)
            .name(name)
            .kind(kind)
            .order(order)
            .build());
    return this;
  }

  public SchemaBuilderImpl column(String name, Column.Kind kind, Column.Order order) {
    checkIndexOnColumn(name);

    Preconditions.checkState(
        materializedViewName != null,
        "This overload of 'column' can only be used with materialized views");
    Preconditions.checkArgument(
        kind == Column.Kind.Clustering, "Order can only be specified for clustering columns");
    Preconditions.checkArgument(order != null, "Clustering order may not be null");
    materializedViewColumns.add(
        ImmutableColumn.builder()
            .keyspace(keyspaceName)
            .table(tableName)
            .name(name)
            .kind(kind)
            .order(order)
            .build());
    return this;
  }

  public SchemaBuilderImpl column(String name, Column.ColumnType type) {
    checkIndexOnColumn(name);
    checkMvOnColumn(name);
    if (type.isUserDefined()) {
      Preconditions.checkArgument(
          udts.stream().anyMatch(u -> u.name().equals(type.name())),
          "User defined type '%s' does not exist in keyspace '%s'",
          type.name(),
          keyspaceName);
    }

    Preconditions.checkState(
        materializedViewName == null && secondaryIndexName == null,
        "This overload of 'column' can only be used with tables or udts");
    columns.add(
        ImmutableColumn.builder()
            .keyspace(keyspaceName)
            .table(tableName)
            .name(name)
            .type(type.dereference(udtKeyspace()))
            .kind(Column.Kind.Regular)
            .build());
    return this;
  }

  public SchemaBuilderImpl column(String name, Class<?> type) {
    checkIndexOnColumn(name);
    checkMvOnColumn(name);

    Preconditions.checkState(
        materializedViewName == null && secondaryIndexName == null,
        "This overload of 'column' can only be used with tables");
    columns.add(
        ImmutableColumn.builder()
            .keyspace(keyspaceName)
            .table(tableName)
            .name(name)
            .type(type)
            .kind(Column.Kind.Regular)
            .build());
    return this;
  }

  public SchemaBuilderImpl column(String name) {
    Preconditions.checkState(
        materializedViewName != null || secondaryIndexName != null,
        "This overload of 'column' can only be used with materialized views or secondary indexes");
    if (secondaryIndexName != null) {
      secondaryIndexColumn = Column.reference(name);
    } else if (materializedViewName != null) {
      materializedViewColumns.add(
          ImmutableColumn.builder()
              .keyspace(keyspaceName)
              .table(tableName)
              .name(name)
              .kind(Column.Kind.Regular)
              .build());
    }
    return this;
  }

  public SchemaBuilderImpl secondaryIndex(String name) {
    finishLast();
    secondaryIndexColumn = null;
    secondaryIndexName = name;
    return this;
  }

  public SchemaBuilderImpl materializedView(String name) {
    finishLast();
    materializedViewName = name;
    return this;
  }

  public SchemaBuilderImpl indexKeys() {
    indexKeys = true;
    return this;
  }

  public SchemaBuilderImpl indexValues() {
    indexValues = true;
    return this;
  }

  public SchemaBuilderImpl indexEntries() {
    indexEntries = true;
    return this;
  }

  public SchemaBuilderImpl indexFull() {
    indexFull = true;
    return this;
  }

  public SchemaBuilderImpl indexClass(String name) {
    indexClass = name;
    return this;
  }

  public SchemaBuilderImpl indexOptions(Map<String, String> options) {
    indexOptions = options;
    return this;
  }

  public SchemaBuilderImpl from(String fromVertex) {
    return this;
  }

  public SchemaBuilderImpl to(String toVertex) {
    return this;
  }

  public SchemaBuilderImpl fromColumn(String... columns) {
    for (String column : columns) {
      fromColumns.add(
          ImmutableColumn.builder().from(columnExistsAndIsPrimaryKey(column).get()).build());
    }
    return this;
  }

  public SchemaBuilderImpl toColumn(String... columns) {
    for (String column : columns) {
      toColumns.add(
          ImmutableColumn.builder().from(columnExistsAndIsPrimaryKey(column).get()).build());
    }
    return this;
  }

  public SchemaBuilderImpl fromColumn(List<String> columns) {
    fromColumn(columns.toArray(new String[0]));
    return this;
  }

  public SchemaBuilderImpl toColumn(List<String> columns) {
    toColumn(columns.toArray(new String[0]));
    return this;
  }

  public SchemaBuilderImpl withReplication(Map<String, String> replication) {
    this.replication = replication;
    return this;
  }

  public SchemaBuilderImpl andDurableWrites(boolean durableWrites) {
    this.durableWrites = Optional.of(durableWrites);
    return this;
  }

  private Optional<Column> columnExistsAndIsPrimaryKey(String name) {
    Optional<Column> column = columns.stream().filter(c -> c.name().equals(name)).findFirst();
    Preconditions.checkArgument(
        column.isPresent(), "Column '%s' does not exist in table '%s'", name, tableName);
    Preconditions.checkArgument(
        column.get().isPrimaryKeyComponent(),
        "Column '%s' is not a primary key in table '%s'",
        name,
        tableName);
    return column;
  }

  private void checkMvOnColumn(String columnName) {
    if (materializedViewName != null) {
      throw new IllegalStateException(
          String.format(
              "Invalid usage of materialized view '%s' on column '%s'",
              materializedViewName, columnName));
    }
  }

  private void checkIndexOnColumn(String columnName) {
    if (secondaryIndexName != null) {
      throw new IllegalStateException(
          String.format(
              "Invalid usage of secondary index '%s' on column '%s'",
              secondaryIndexName, columnName));
    }
  }

  private void finishLastSecondaryIndex() {
    if (secondaryIndexName != null) {
      Preconditions.checkArgument(
          secondaryIndexColumn != null, "No column is referenced for secondary index");

      Optional<Column> sourceColumn =
          columns.stream().filter(c -> c.name().equals(secondaryIndexColumn.name())).findFirst();
      Preconditions.checkArgument(
          sourceColumn.isPresent(),
          "Secondary index references unknown column '%s'",
          secondaryIndexColumn.name());

      if (sourceColumn.get().isFrozenCollection()) {
        Preconditions.checkArgument(
            indexFull,
            "Only indexFull() is supported on frozen collection '%s'",
            sourceColumn.get().name());
      } else if (sourceColumn.get().ofTypeListOrSet()) {
        Preconditions.checkArgument(
            !indexFull,
            "indexFull() cannot be applied to column '%s'. It can only be used on a frozen collection",
            sourceColumn.get().name());
        Preconditions.checkArgument(
            !indexKeys,
            "indexKeys() cannot be applied to column '%s'. It can only be used on a Map",
            sourceColumn.get().name());
        Preconditions.checkArgument(
            !indexEntries,
            "indexEntries() cannot be applied to column '%s'. It can only be used on a Map",
            sourceColumn.get().name());
        // indexValues is the default for a Set/List if no indexing type is specified
        indexValues = true;
      } else if (sourceColumn.get().ofTypeMap()) {
        Preconditions.checkArgument(
            !indexFull,
            "indexFull() cannot be applied to column '%s'. It can only be used on a frozen collection",
            sourceColumn.get().name());

        if (!indexEntries && !indexKeys && !indexValues) {
          // indexValues is the default if no indexing type is specified
          indexValues = true;
        }
      }

      indexes.add(
          SecondaryIndex.create(
              keyspaceName,
              secondaryIndexName,
              sourceColumn.get(),
              ImmutableCollectionIndexingType.builder()
                  .indexEntries(indexEntries)
                  .indexKeys(indexKeys)
                  .indexValues(indexValues)
                  .indexFull(indexFull)
                  .build(),
              indexClass,
              indexOptions));

      secondaryIndexColumn = null;
      secondaryIndexName = null;
      indexKeys = false;
      indexValues = false;
      indexEntries = false;
      indexFull = false;
      indexClass = null;
      if (indexOptions != null) {
        indexOptions.clear();
      }
    }
  }

  private void finishLastMaterializedView() {
    if (materializedViewName != null) {
      finishLast();
      Preconditions.checkArgument(
          !materializedViewColumns.isEmpty(), "No column is referenced for materialized view");

      List<Column> columnReferences = new ArrayList<>();
      for (Column col : materializedViewColumns) {
        Optional<Column> sourceColumn =
            columns.stream().filter(c -> c.name().equals(col.name())).findFirst();
        Preconditions.checkArgument(
            sourceColumn.isPresent(),
            "Materialized view references unknown column '%s'",
            col.name());
        columnReferences.add(
            ImmutableColumn.builder()
                .name(sourceColumn.get().name())
                .type(sourceColumn.get().type())
                .kind(col.kind())
                .order(col.order())
                .build());
      }

      Set<String> columnNames =
          materializedViewColumns.stream().map(c -> c.name()).collect(Collectors.toSet());
      Set<String> sourceTablePkColumnNames =
          columns.stream()
              .filter(
                  c -> c.kind() == Column.Kind.PartitionKey || c.kind() == Column.Kind.Clustering)
              .map(c -> c.name())
              .collect(Collectors.toSet());
      Sets.SetView<String> missingColumns = Sets.difference(sourceTablePkColumnNames, columnNames);
      Preconditions.checkArgument(
          missingColumns.isEmpty(), "Materialized view was missing PK columns %s", missingColumns);

      indexes.add(
          MaterializedView.create(
              keyspaceName, materializedViewName, ImmutableList.copyOf(columnReferences)));
      materializedViewColumns.clear();
      materializedViewName = null;
    }
  }

  public Schema build() {
    finishLastUDT();
    keyspace(null);
    Schema schema = Schema.create(ImmutableSet.copyOf(keyspaces));
    callback.ifPresent(c -> c.accept(schema));
    return schema;
  }
}
