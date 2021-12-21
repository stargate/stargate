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
import com.github.misberner.duzzt.annotations.DSLAction;
import com.github.misberner.duzzt.annotations.GenerateEmbeddedDSL;
import com.github.misberner.duzzt.annotations.SubExpr;
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
@GenerateEmbeddedDSL(
    autoVarArgs = false,
    name = "SchemaBuilder",
    syntax = "<keyspace>* build",
    where = {
      @SubExpr(
          name = "keyspace",
          definedAs = "keyspace ((withReplication? andDurableWrites?) <type>* <table>*)"),
      @SubExpr(
          name = "table",
          definedAs =
              "table (column)+ (secondaryIndex column (indexKeys|indexValues|indexEntries|indexFull)? (indexClass indexOptions?)?)* <materializedView>*"),
      @SubExpr(name = "materializedView", definedAs = "materializedView (column)+"),
      @SubExpr(name = "type", definedAs = "type (column)+"),
    })
public class SchemaBuilderImpl {

  private final Set<Keyspace> keyspaces = new LinkedHashSet<>();
  private final Set<Table> tables = new LinkedHashSet<>();
  private final Set<Column> columns = new LinkedHashSet<>();
  private final Set<Column> materializedViewColumns = new LinkedHashSet<>();
  private final Set<Index> indexes = new LinkedHashSet<>();
  private String keyspaceName;
  private String tableName;
  private String materializedViewName;
  private Column secondaryIndexColumn;
  private String secondaryIndexName;
  private final Optional<Consumer<Schema>> callback;
  private String udtTypeName;
  private final Set<UserDefinedType> udts = new LinkedHashSet<>();
  private boolean indexKeys;
  private boolean indexValues;
  private boolean indexEntries;
  private boolean indexFull;
  private String indexClass;
  private Map<String, String> indexOptions = new HashMap<>();
  private final List<Column> fromColumns = new ArrayList<>();
  private final List<Column> toColumns = new ArrayList<>();
  private Map<String, String> replication = Collections.emptyMap();
  private Optional<Boolean> durableWrites = Optional.empty();

  public SchemaBuilderImpl(Optional<Consumer<Schema>> callback) {
    this.callback = callback;
  }

  @DSLAction
  public void keyspace(String name) {
    if (keyspaceName != null) {
      table(null);

      keyspaces.add(Keyspace.create(keyspaceName, tables, udts, replication, durableWrites));
      tables.clear();
      udts.clear();

      replication = Collections.emptyMap();
      durableWrites = Optional.empty();
    }
    keyspaceName = name;
  }

  @DSLAction
  public void type(String typeName) {
    finishLast();
    this.udtTypeName = typeName;
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

  @DSLAction
  public void table(String name) {
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

  @DSLAction
  public void column(String name, Column.ColumnType type, Column.Kind kind) {
    column(name, type, kind, kind == Column.Kind.Clustering ? Column.Order.ASC : null);
  }

  @DSLAction
  public void column(String name, Column.ColumnType type, Column.Kind kind, Column.Order order) {
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
  }

  @DSLAction
  public void column(String name, Class type, Column.Kind kind) {
    column(name, type, kind, kind == Column.Kind.Clustering ? Column.Order.ASC : null);
  }

  @DSLAction
  public void column(String name, Class type, Column.Kind kind, Column.Order order) {
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
  }

  @DSLAction
  public void column(String name, Column.Kind kind) {
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
  }

  @DSLAction
  public void column(String name, Column.Kind kind, Column.Order order) {
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
  }

  @DSLAction
  public void column(String name, Column.ColumnType type) {
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
  }

  @DSLAction
  public void column(String name, Class<?> type) {
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
  }

  @DSLAction
  public void column(String name) {
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
  }

  @DSLAction
  public void secondaryIndex(String name) {
    finishLast();
    secondaryIndexColumn = null;
    secondaryIndexName = name;
  }

  @DSLAction
  public void materializedView(String name) {
    finishLast();
    materializedViewName = name;
  }

  @DSLAction
  public void indexKeys() {
    indexKeys = true;
  }

  @DSLAction
  public void indexValues() {
    indexValues = true;
  }

  @DSLAction
  public void indexEntries() {
    indexEntries = true;
  }

  @DSLAction
  public void indexFull() {
    indexFull = true;
  }

  @DSLAction
  public void indexClass(String name) {
    indexClass = name;
  }

  @DSLAction
  public void indexOptions(Map<String, String> options) {
    indexOptions = options;
  }

  @DSLAction
  public void from(String fromVertex) {}

  @DSLAction
  public void to(String toVertex) {}

  @DSLAction
  public void fromColumn(String... columns) {
    for (String column : columns) {
      fromColumns.add(
          ImmutableColumn.builder().from(columnExistsAndIsPrimaryKey(column).get()).build());
    }
  }

  @DSLAction
  public void toColumn(String... columns) {
    for (String column : columns) {
      toColumns.add(
          ImmutableColumn.builder().from(columnExistsAndIsPrimaryKey(column).get()).build());
    }
  }

  @DSLAction
  public void fromColumn(List<String> columns) {
    fromColumn(columns.toArray(new String[0]));
  }

  @DSLAction
  public void toColumn(List<String> columns) {
    toColumn(columns.toArray(new String[0]));
  }

  @DSLAction
  public void withReplication(Map<String, String> replication) {
    this.replication = replication;
  }

  @DSLAction
  public void andDurableWrites(boolean durableWrites) {
    this.durableWrites = Optional.of(durableWrites);
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

  @DSLAction
  public Schema build() {
    finishLastUDT();
    keyspace(null);
    Schema schema = Schema.create(ImmutableSet.copyOf(keyspaces));
    callback.ifPresent(c -> c.accept(schema));
    return schema;
  }
}
