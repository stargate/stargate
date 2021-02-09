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
package io.stargate.db.query.builder;

import static com.datastax.oss.driver.shaded.guava.common.base.Preconditions.checkArgument;
import static io.stargate.db.query.BindMarker.markerFor;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.github.misberner.apcommons.util.AFModifier;
import com.github.misberner.duzzt.annotations.DSLAction;
import com.github.misberner.duzzt.annotations.GenerateEmbeddedDSL;
import com.github.misberner.duzzt.annotations.SubExpr;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BindMarker;
import io.stargate.db.query.Modification.Operation;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.CollectionIndexingType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ColumnUtils;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.SchemaEntity;
import io.stargate.db.schema.SecondaryIndex;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.javatuples.Pair;

/** Convenience builder for creating queries. */
@GenerateEmbeddedDSL(
    modifier = AFModifier.DEFAULT,
    autoVarArgs = false,
    name = "QueryBuilder",
    syntax = "(<keyspace>|<table>|<insert>|<update>|<delete>|<select>|<index>|<type>) build",
    where = {
      @SubExpr(
          name = "keyspace",
          definedAs = "<keyspaceCreate> | <keyspaceAlter> | (drop keyspace ifExists?)"),
      @SubExpr(
          name = "keyspaceCreate",
          definedAs = "(create keyspace ifNotExists? withReplication andDurableWrites?)"),
      @SubExpr(
          name = "keyspaceAlter",
          definedAs = "(alter keyspace (withReplication andDurableWrites?)?)"),
      @SubExpr(
          name = "table",
          definedAs =
              "(create table ifNotExists? column+ withComment? withDefaultTTL?) | (alter table (((addColumn+)? | (dropColumn+)? | (renameColumn+)?) | (withComment? withDefaultTTL?))) | (drop table ifExists?) | (truncate table)"),
      @SubExpr(
          name = "type",
          definedAs =
              "(create type ifNotExists?) | (drop type ifExists?) | (alter type addColumn+)"),
      @SubExpr(name = "insert", definedAs = "insertInto value+ ifNotExists? ttl? timestamp?"),
      @SubExpr(name = "update", definedAs = "update ttl? timestamp? value+ where+ ifs* ifExists?"),
      @SubExpr(name = "delete", definedAs = "delete column* from timestamp? where+ ifs* ifExists?"),
      @SubExpr(
          name = "select",
          definedAs =
              "select star? column* writeTimeColumn? from (where* limit? orderBy*) allowFiltering?"),
      @SubExpr(
          name = "index",
          definedAs =
              "(drop ((materializedView|index) ifExists?)) | (create ((materializedView ifNotExists? asSelect (column+) from)"
                  + " | (custom? index ifNotExists? on column (indexKeys|indexValues|indexEntries|indexFull|indexingType)?)))"),
    })
public class QueryBuilderImpl {
  private final Schema schema;
  private final Codec valueCodec;
  private final @Nullable AsyncQueryExecutor executor;

  private int markerIndex;

  private boolean isCreate;
  private boolean isAlter;
  private boolean isInsert;
  private boolean isUpdate;
  private boolean isDelete;
  private boolean isSelect;
  private boolean isDrop;
  private boolean isKeyspace;
  private boolean isTable;
  private boolean isMaterializedView;
  private boolean isType;
  private boolean isIndex;
  private boolean isTruncate;
  private boolean indexKeys;
  private boolean indexValues;
  private boolean indexEntries;
  private boolean indexFull;

  private String keyspaceName;
  private String tableName;
  private String indexName;

  private final List<Column> createColumns = new ArrayList<>();

  private final List<Column> addColumns = new ArrayList<>();
  private final List<String> dropColumns = new ArrayList<>();
  private final List<Pair<String, String>> columnRenames = new ArrayList<>();

  /**
   * The modifications made for a DML query (for INSERT, this will include modifications for the
   * primary key columns, but for UPDATE those will be part of the WHERE clause; note that for
   * DELETE, nothing will be populated by the builder since {@link #selection} and {@link #wheres}
   * are used instead).
   */
  private final List<ValueModifier> dmlModifications = new ArrayList<>();

  /** Column names for a SELECT or DELETE. */
  private final List<String> selection = new ArrayList<>();

  /** The where conditions for a SELECT or UPDATE. */
  private final List<BuiltCondition> wheres = new ArrayList<>();

  /** The IFs conditions for a conditional UPDATE or DELETE. */
  private final List<BuiltCondition> ifs = new ArrayList<>();

  private @Nullable Value<Long> limit;
  private List<ColumnOrder> orders = new ArrayList<>();

  private Replication replication;
  private boolean ifNotExists;
  private boolean ifExists;
  private @Nullable Boolean durableWrites;
  private String comment;
  private Integer defaultTTL;
  private String indexCreateColumn;
  private String customIndexClass;
  private UserDefinedType type;
  private Value<Integer> ttl;
  private Value<Long> timestamp;
  private String writeTimeColumn;
  private String writeTimeColumnAlias;
  private boolean allowFiltering;

  public QueryBuilderImpl(Schema schema, Codec valueCodec, @Nullable AsyncQueryExecutor executor) {
    this.schema = schema;
    this.valueCodec = valueCodec;
    this.executor = executor;
  }

  private void preprocessValue(Value<?> v) {
    if (v != null && v.isMarker()) {
      ((Value.Marker<?>) v).setExternalIndex(markerIndex++);
    }
  }

  @DSLAction
  public void create() {
    isCreate = true;
  }

  @DSLAction
  public void alter() {
    isAlter = true;
  }

  @DSLAction
  public void drop() {
    isDrop = true;
  }

  @DSLAction
  public void truncate() {
    isTruncate = true;
  }

  @DSLAction
  public void keyspace(String keyspace) {
    this.keyspaceName = keyspace;
    this.isKeyspace = true;
  }

  @DSLAction
  public void table(String keyspace, String table) {
    this.keyspaceName = keyspace;
    table(table);
  }

  @DSLAction
  public void table(String table) {
    checkArgument(keyspaceName != null, "Keyspace must be specified");
    this.tableName = table;
    this.isTable = true;
  }

  @DSLAction
  public void withReplication(Replication replication) {
    this.replication = replication;
  }

  @DSLAction
  public void andDurableWrites(boolean durableWrites) {
    this.durableWrites = durableWrites;
  }

  @DSLAction
  public void ifNotExists() {
    ifNotExists(true);
  }

  public void ifNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @DSLAction
  public void ifExists() {
    ifExists(true);
  }

  public void ifExists(boolean ifExists) {
    this.ifExists = ifExists;
  }

  @DSLAction
  public void withComment(String comment) {
    this.comment = comment;
  }

  @DSLAction
  public void withDefaultTTL(int defaultTTL) {
    this.defaultTTL = defaultTTL;
  }

  @DSLAction
  public void column(String column) {
    if (isCreate) {
      checkArgument(
          !isTable, "Column '%s' type must be specified for table '%s'", column, tableName);

      if (isMaterializedView) {
        createColumns.add(Column.reference(column));
      } else if (isIndex) {
        // The DSL should only allow to call this once for a CREATE INDEX, but making sure.
        checkArgument(indexCreateColumn == null, "Index column has already been set");
        indexCreateColumn = column;
      } else {
        // We should haven't other case where this can be called ...
        throw new AssertionError("This shouldn't have been called");
      }
    } else if (isSelect || isDelete) {
      selection.add(column);
    } else {
      // We should haven't other case where this can be called ...
      throw new AssertionError("This shouldn't have been called");
    }
  }

  public void column(String... columns) {
    for (String c : columns) {
      column(c);
    }
  }

  public void column(Column column) {
    checkMaterializedViewColumn(column);
    checkTableColumn(column);
    if (isCreate && (isTable || isMaterializedView)) {
      createColumns.add(column);
    } else {
      column(column.name());
    }
  }

  public void column(Collection<Column> columns) {
    for (Column c : columns) {
      column(c);
    }
  }

  @DSLAction
  public void column(String column, Column.ColumnType type, Column.Kind kind) {
    column(ImmutableColumn.builder().name(column).type(type).kind(kind).build());
  }

  @DSLAction
  public void column(String column, Column.ColumnType type, Column.Kind kind, Column.Order order) {
    column(ImmutableColumn.builder().name(column).type(type).kind(kind).order(order).build());
  }

  @DSLAction
  public void column(String column, Class<?> type, Column.Kind kind) {
    column(ImmutableColumn.builder().name(column).type(type).kind(kind).build());
  }

  @DSLAction
  public void column(String column, Class<?> type, Column.Kind kind, Column.Order order) {
    column(ImmutableColumn.builder().name(column).type(type).kind(kind).order(order).build());
  }

  @DSLAction
  public void column(String column, Column.Kind kind) {
    column(ImmutableColumn.builder().name(column).kind(kind).build());
  }

  @DSLAction
  public void column(String column, Column.Kind kind, Column.Order order) {
    column(ImmutableColumn.builder().name(column).kind(kind).order(order).build());
  }

  @DSLAction
  public void column(String column, Column.ColumnType type) {
    column(column, type, Column.Kind.Regular);
  }

  public void writeTimeColumn(String columnName) {
    writeTimeColumn(columnName, null);
  }

  public void writeTimeColumn(String columnName, String alias) {
    this.writeTimeColumn = columnName;
    this.writeTimeColumnAlias = alias;
  }

  public void star() {
    // This can be called to be explicit, but is the default when no columns are selected
    checkArgument(this.selection.isEmpty(), "Cannot use * when other columns are selected");
  }

  private void checkMaterializedViewColumn(Column column) {
    checkArgument(
        !isCreate || !isMaterializedView || column.type() == null,
        "Column '%s' type should not be specified for materialized view '%s'",
        column.name(),
        indexName != null ? indexName : "");
  }

  private void checkTableColumn(Column column) {
    checkArgument(
        !isCreate || !isTable || column.type() != null,
        "Column '%s' type must be specified for table '%s'",
        column.name(),
        tableName != null ? tableName : "");
  }

  @DSLAction
  public void column(String column, Class<?> type) {
    column(column, type, Column.Kind.Regular);
  }

  @DSLAction
  public void addColumn(String column, Column.ColumnType type) {
    addColumn(ImmutableColumn.builder().name(column).type(type).kind(Column.Kind.Regular).build());
  }

  public void addColumn(Column column) {
    addColumns.add(column);
  }

  public void addColumn(Collection<Column> columns) {
    for (Column column : columns) {
      addColumn(column);
    }
  }

  @DSLAction
  public void dropColumn(String column) {
    dropColumns.add(column);
  }

  public void dropColumn(Collection<String> columns) {
    for (String column : columns) {
      dropColumn(column);
    }
  }

  public void dropColumn(Column column) {
    dropColumn(column.name());
  }

  @DSLAction
  public void renameColumn(String from, String to) {
    columnRenames.add(Pair.with(from, to));
  }

  @DSLAction
  public void insertInto(String keyspace, String table) {
    this.keyspaceName = keyspace;
    this.tableName = table;
    this.isInsert = true;
  }

  public void insertInto(Table table) {
    insertInto(table.keyspace(), table.name());
  }

  @DSLAction
  public void update(String keyspace, String table) {
    this.keyspaceName = keyspace;
    this.tableName = table;
    this.isUpdate = true;
  }

  public void update(Table table) {
    update(table.keyspace(), table.name());
  }

  @DSLAction
  public void delete() {
    this.isDelete = true;
  }

  @DSLAction
  public void select() {
    this.isSelect = true;
  }

  @DSLAction
  public void from(String keyspace, String table) {
    this.keyspaceName = keyspace;
    from(table);
  }

  @DSLAction
  public void from(String table) {
    checkArgument(keyspaceName != null, "Keyspace must be specified");
    this.tableName = table;
  }

  public void from(AbstractTable table) {
    from(table.keyspace(), table.name());
  }

  @DSLAction
  public void value(String column, Object value) {
    value(ValueModifier.set(column, value));
  }

  @DSLAction
  public void value(String column) {
    value(ValueModifier.marker(column));
  }

  public void value(Column column) {
    value(column.name());
  }

  public void value(Column column, Object value) {
    value(column.name(), value);
  }

  public void value(ValueModifier modifier) {
    preprocessValue(modifier.target().mapKey());
    preprocessValue(modifier.value());
    dmlModifications.add(modifier);
  }

  public void value(Collection<ValueModifier> setters) {
    for (ValueModifier setter : setters) {
      value(setter);
    }
  }

  public void where(Column column, Predicate predicate, Object value) {
    where(column.name(), predicate, value);
  }

  public void where(Column column, Predicate predicate) {
    where(column.name(), predicate);
  }

  public void where(String columnName, Predicate predicate, Object value) {
    where(BuiltCondition.of(columnName, predicate, value));
  }

  public void where(String columnName, Predicate predicate) {
    where(BuiltCondition.ofMarker(columnName, predicate));
  }

  public void where(BuiltCondition where) {
    where.lhs().value().ifPresent(this::preprocessValue);
    preprocessValue(where.value());
    wheres.add(where);
  }

  @DSLAction(autoVarArgs = false)
  public void where(Collection<? extends BuiltCondition> where) {
    for (BuiltCondition condition : where) {
      where(condition);
    }
  }

  public void ifs(String columnName, Predicate predicate, Object value) {
    ifs(BuiltCondition.of(columnName, predicate, value));
  }

  public void ifs(String columnName, Predicate predicate) {
    ifs(BuiltCondition.ofMarker(columnName, predicate));
  }

  public void ifs(BuiltCondition condition) {
    condition.lhs().value().ifPresent(this::preprocessValue);
    preprocessValue(condition.value());
    ifs.add(condition);
  }

  @DSLAction(autoVarArgs = false)
  public void ifs(Collection<? extends BuiltCondition> conditions) {
    for (BuiltCondition condition : conditions) {
      ifs(condition);
    }
  }

  @DSLAction
  public void materializedView(String keyspace, String name) {
    this.keyspaceName = keyspace;
    materializedView(name);
  }

  @DSLAction
  public void materializedView(String name) {
    checkArgument(keyspaceName != null, "Keyspace must be specified");
    // Note that we use the index to store the MV name, because the table variable will be used
    // to store the base table name.
    this.indexName = name;
    this.isMaterializedView = true;
  }

  public void materializedView(Keyspace keyspace, String name) {
    materializedView(keyspace.name(), name);
  }

  @DSLAction
  public void asSelect() {
    // This method is just so the builder flows better
  }

  @DSLAction
  public void on(String keyspace, String table) {
    this.keyspaceName = keyspace;
    on(table);
  }

  @DSLAction
  public void on(String table) {
    checkArgument(keyspaceName != null, "Keyspace must be specified");
    this.tableName = table;
  }

  public void on(Table table) {
    on(table.keyspace(), table.name());
  }

  @DSLAction
  public void index(String index) {
    this.indexName = index;
    this.isIndex = true;
  }

  @DSLAction
  public void index() {
    index((String) null);
  }

  public void index(SecondaryIndex index) {
    index(index.name());
  }

  @DSLAction
  public void index(String keyspace, String index) {
    this.keyspaceName = keyspace;
    index(index);
  }

  public void index(Keyspace keyspace, SecondaryIndex index) {
    index(keyspace.name(), index.name());
  }

  @DSLAction
  public void indexingType(CollectionIndexingType indexingType) {
    if (indexingType.indexEntries()) {
      indexEntries();
    } else if (indexingType.indexFull()) {
      indexFull();
    } else if (indexingType.indexKeys()) {
      indexKeys();
    } else if (indexingType.indexValues()) {
      indexValues();
    }
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
  public void custom(String customIndexClass) {
    this.customIndexClass = customIndexClass;
  }

  @DSLAction
  public void type(String keyspace, UserDefinedType type) {
    this.keyspaceName = keyspace;
    this.type = type;
    this.isType = true;
  }

  @DSLAction
  public void limit() {
    this.limit = Value.marker();
    preprocessValue(this.limit);
  }

  @DSLAction
  public void limit(Long limit) {
    if (limit != null) {
      this.limit = Value.of(limit);
    }
  }

  public void orderBy(Column column, Column.Order order) {
    orderBy(column.name(), order);
  }

  public void orderBy(ColumnOrder... orders) {
    Collections.addAll(this.orders, orders);
  }

  public void orderBy(List<ColumnOrder> orders) {
    this.orders = orders;
  }

  public void orderBy(String column, Column.Order order) {
    orderBy(ColumnOrder.of(column, order));
  }

  public void orderBy(String column) {
    orderBy(column, Column.Order.ASC);
  }

  public void orderBy(Column column) {
    orderBy(column, Column.Order.ASC);
  }

  public void allowFiltering() {
    this.allowFiltering = true;
  }

  public void allowFiltering(boolean allowFiltering) {
    this.allowFiltering = allowFiltering;
  }

  @DSLAction
  public void ttl() {
    this.ttl = Value.marker();
    preprocessValue(this.ttl);
  }

  @DSLAction
  public void ttl(Integer ttl) {
    if (ttl != null) {
      this.ttl = Value.of(ttl);
    }
  }

  @DSLAction
  public void timestamp() {
    this.timestamp = Value.marker();
    preprocessValue(this.timestamp);
  }

  @DSLAction
  public void timestamp(Long timestamp) {
    if (timestamp != null) {
      this.timestamp = Value.of(timestamp);
    }
  }

  @DSLAction
  public BuiltQuery<?> build() {
    if (isKeyspace && isCreate) {
      return createKeyspace();
    }
    if (isKeyspace && isAlter) {
      return alterKeyspace();
    }
    if (isKeyspace && isDrop) {
      return dropKeyspace();
    }

    if (isTable && isCreate) {
      return createTable();
    }
    if (isTable && isAlter) {
      return alterTable();
    }
    if (isTable && isDrop) {
      return dropTable();
    }
    if (isTable && isTruncate) {
      return truncateTable();
    }

    if (isIndex && isCreate) {
      return createIndex();
    }
    if (isIndex && isDrop) {
      return dropIndex();
    }

    if (isMaterializedView && isCreate) {
      return createMaterializedView();
    }
    if (isMaterializedView && isDrop) {
      return dropMaterializedView();
    }

    if (isType && isCreate) {
      return createType();
    }
    if (isType && isDrop) {
      return dropType();
    }
    if (isType && isAlter) {
      return alterType();
    }

    if (isInsert) {
      return insertQuery();
    }
    if (isUpdate) {
      return updateQuery();
    }
    if (isDelete) {
      return deleteQuery();
    }
    if (isSelect) {
      return selectQuery();
    }

    throw new AssertionError("Unknown query type");
  }

  @FormatMethod
  private static IllegalArgumentException invalid(@FormatString String format, Object... args) {
    return new IllegalArgumentException(format(format, args));
  }

  private Keyspace schemaKeyspace() {
    Keyspace ks = schema.keyspace(keyspaceName);
    if (ks == null) {
      throw invalid("Unknown keyspace %s", cqlName(keyspaceName));
    }
    return ks;
  }

  private Table schemaTable() {
    Table table = schemaKeyspace().table(tableName);
    if (table == null) {
      throw invalid("Unknown table %s.%s ", cqlName(keyspaceName), cqlName(tableName));
    }
    return table;
  }

  private static String cqlName(String name) {
    return ColumnUtils.maybeQuote(name);
  }

  private static class WithAdder {
    private final StringBuilder builder;
    private boolean withAdded;

    private WithAdder(StringBuilder builder) {
      this.builder = builder;
    }

    private StringBuilder add() {
      if (!withAdded) {
        builder.append(" WITH");
        withAdded = true;
      } else {
        builder.append(" AND");
      }
      return builder;
    }
  }

  private BuiltQuery<?> createKeyspace() {
    StringBuilder query = new StringBuilder();
    query.append("CREATE KEYSPACE ");
    String ksName = cqlName(keyspaceName);
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    }
    query.append(ksName);

    query.append(" WITH replication = ").append(replication);
    if (durableWrites != null) {
      query.append(" AND durable_writes = ").append(durableWrites);
    }

    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private BuiltQuery<?> alterKeyspace() {
    StringBuilder query = new StringBuilder();
    Keyspace keyspace = schemaKeyspace();
    query.append("ALTER KEYSPACE ").append(keyspace.cqlName());

    WithAdder with = new WithAdder(query);
    if (null != replication) {
      with.add().append(" replication = ").append(replication);
    }
    if (null != durableWrites) {
      with.add().append(" durable_writes = ").append(durableWrites);
    }

    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private BuiltQuery<?> dropKeyspace() {
    StringBuilder query = new StringBuilder();
    query.append("DROP KEYSPACE ");
    String ksName = cqlName(keyspaceName);
    if (ifExists) {
      query.append("IF EXISTS ");
    } else if (schema.keyspace(keyspaceName) == null) {
      throw invalid("Keyspace %s does not exists", ksName);
    }
    query.append(ksName);

    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private void addPrimaryKey(StringBuilder query, List<Column> columns, String name) {
    checkArgument(
        columns.stream()
            .anyMatch(c -> c.kind() == Column.Kind.valueOf(Column.Kind.PartitionKey.name())),
        "At least one partition key must be specified for table or materialized view '%s' %s",
        name,
        Arrays.deepToString(columns.toArray()));
    query.append("PRIMARY KEY ((");
    query.append(
        columns.stream()
            .filter(c -> c.kind() == Column.Kind.PartitionKey)
            .map(SchemaEntity::cqlName)
            .collect(Collectors.joining(", ")));
    query.append(")");
    if (columns.stream().anyMatch(c -> c.kind() == Column.Kind.Clustering)) {
      query.append(", ");
    }
    query.append(
        columns.stream()
            .filter(c -> c.kind() == Column.Kind.Clustering)
            .map(SchemaEntity::cqlName)
            .collect(Collectors.joining(", ")));
    query.append(")");
  }

  private void addClusteringOrder(WithAdder with, List<Column> columns) {
    if (columns.stream().anyMatch(c -> c.kind() == Column.Kind.Clustering && c.order() != null)) {
      StringBuilder query = with.add();
      query.append(" CLUSTERING ORDER BY (");
      query.append(
          columns.stream()
              .filter(c -> c.kind() == Column.Kind.Clustering)
              .map(c -> c.cqlName() + " " + c.order().name().toUpperCase())
              .collect(Collectors.joining(", ")));
      query.append(")");
    }
  }

  private void addComment(WithAdder with) {
    if (comment != null) {
      with.add().append(" comment = '").append(comment).append("'");
    }
  }

  private void addDefaultTTL(WithAdder with) {
    if (defaultTTL != null) {
      with.add().append(" default_time_to_live = ").append(defaultTTL);
    }
  }

  private BuiltQuery<?> createTable() {
    StringBuilder query = new StringBuilder();
    Keyspace ks = schemaKeyspace();
    String tableName = cqlName(this.tableName);
    query.append("CREATE TABLE ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    } else if (ks.table(this.tableName) != null) {
      throw invalid("A table named %s already exists", tableName);
    }
    query.append(ks.cqlName()).append('.').append(tableName);
    query.append(" (");
    query.append(
        createColumns.stream()
            .map(
                c ->
                    c.cqlName()
                        + " "
                        + c.type().cqlDefinition()
                        + (c.kind() == Column.Kind.Static ? " STATIC" : ""))
            .collect(Collectors.joining(", ")));
    query.append(", ");
    addPrimaryKey(query, createColumns, tableName);
    query.append(")");

    WithAdder with = new WithAdder(query);
    addClusteringOrder(with, createColumns);
    addComment(with);
    addDefaultTTL(with);

    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private void addName(StringBuilder query, Table table) {
    query.append(table.cqlKeyspace()).append('.').append(table.cqlName());
  }

  private BuiltQuery<?> alterTable() {
    StringBuilder query = new StringBuilder();
    Table table = schemaTable();
    addName(query.append("ALTER TABLE "), table);

    if (!addColumns.isEmpty()) {
      query.append(" ADD (");
      query.append(
          addColumns.stream()
              .map(
                  c ->
                      c.cqlName()
                          + " "
                          + c.type().cqlDefinition()
                          + (c.kind() == Column.Kind.Static ? " STATIC" : ""))
              .collect(Collectors.joining(", ")));
      query.append(")");
    }
    if (!dropColumns.isEmpty()) {
      query.append(" DROP (");
      query.append(
          dropColumns.stream().map(QueryBuilderImpl::cqlName).collect(Collectors.joining(", ")));
      query.append(")");
    }
    if (!columnRenames.isEmpty()) {
      query.append(" RENAME ");
      boolean isFirst = true;
      for (Pair<String, String> rename : columnRenames) {
        if (isFirst) isFirst = false;
        else query.append(" AND ");
        query
            .append(cqlName(rename.getValue0()))
            .append(" TO ")
            .append(cqlName(rename.getValue1()));
      }
    }
    WithAdder with = new WithAdder(query);
    addComment(with);
    addDefaultTTL(with);
    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private BuiltQuery<?> dropTable() {
    StringBuilder query = new StringBuilder();
    Keyspace keyspace = schemaKeyspace();
    String name = cqlName(tableName);
    query.append("DROP TABLE ");
    if (ifExists) {
      query.append("IF EXISTS ");
    } else if (keyspace.table(tableName) == null) {
      throw invalid("Table %s.%s does not exists", keyspace.cqlName(), name);
    }
    query.append(keyspace.cqlName()).append('.').append(name);
    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private BuiltQuery<?> truncateTable() {
    StringBuilder query = new StringBuilder();
    query.append("TRUNCATE ");
    addName(query, schemaTable());
    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private boolean indexExists(Keyspace keyspace, String indexName) {
    return keyspace.tables().stream()
        .flatMap(t -> t.indexes().stream())
        .anyMatch(i -> indexName.equals(i.name()));
  }

  private BuiltQuery<?> createIndex() {
    StringBuilder query = new StringBuilder();
    Table table = schemaTable();
    query.append("CREATE");
    if (customIndexClass != null) {
      query.append(" CUSTOM");
    }
    query.append(" INDEX");
    if (ifNotExists) {
      query.append(" IF NOT EXISTS");
    } else if (indexName != null && indexExists(schemaKeyspace(), indexName)) {
      throw invalid("An index named %s already exists", indexName);
    }
    if (indexName != null) {
      query.append(" " + cqlName(indexName));
    }
    query.append(" ON ");
    addName(query, table);
    query.append(" (");
    Column column = table.column(indexCreateColumn);
    if (column == null) {
      throw invalid(
          "Unknown column %s in table %s.%s",
          indexCreateColumn, table.cqlKeyspace(), table.cqlName());
    }
    if (indexKeys) {
      checkArgument(column.ofTypeMap(), "Indexing keys can only be used with a map");
      query.append("KEYS(");
    } else if (indexValues) {
      checkArgument(column.isCollection(), "Indexing values can only be used on collections");
      query.append("VALUES(");
    } else if (indexEntries) {
      checkArgument(column.ofTypeMap(), "Indexing entries can only be used with a map");
      query.append("ENTRIES(");
    } else if (indexFull) {
      query.append("FULL(");
      checkArgument(
          column.isFrozenCollection(), "Full indexing can only be used with a frozen list/map/set");
    }
    query.append(column.cqlName());
    if (indexKeys || indexValues || indexEntries || indexFull) {
      query.append(")");
    }
    query.append(")");
    if (customIndexClass != null) {
      query.append(" USING").append(format(" '%s'", customIndexClass));
    }
    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private BuiltQuery<?> dropIndex() {
    StringBuilder query = new StringBuilder();
    Keyspace keyspace = schemaKeyspace();
    query.append("DROP INDEX ");
    if (ifExists) {
      query.append("IF EXISTS ");
    } else if (!indexExists(keyspace, indexName)) {
      throw invalid("Index %s does not exists", indexName);
    }
    query.append(keyspace.cqlName()).append('.').append(cqlName(indexName));
    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private static List<Column> validateColumns(Table table, Collection<String> columnNames) {
    return columnNames.stream().map(table::existingColumn).collect(toList());
  }

  private static List<Column> convertToColumns(Table table, Collection<String> columnNames) {
    return columnNames.stream().map(table::column).collect(toList());
  }

  private Set<String> names(Collection<Column> columns) {
    return columns.stream().map(SchemaEntity::name).collect(toSet());
  }

  private BuiltQuery<?> createMaterializedView() {
    StringBuilder query = new StringBuilder();
    Table baseTable = schemaTable();
    Set<String> baseTablePkColumnNames = names(baseTable.primaryKeyColumns());
    Set<String> mvColumnNames = names(createColumns);
    // This will throw if one of column is not unknown in the base table
    validateColumns(baseTable, mvColumnNames);
    Sets.SetView<String> missingColumns = Sets.difference(baseTablePkColumnNames, mvColumnNames);
    checkArgument(
        missingColumns.isEmpty(),
        "Materialized view %s primary key was missing components %s",
        cqlName(indexName),
        missingColumns);
    List<String> mvPrimaryKeyColumns =
        createColumns.stream()
            .filter(c -> c.kind() != null && c.isPrimaryKeyComponent())
            .map(SchemaEntity::name)
            .collect(toList());
    checkArgument(
        !mvPrimaryKeyColumns.isEmpty(),
        "Materialized view %s must have at least 1 primary key column",
        cqlName(indexName));

    List<String> nonPrimaryKeysThatArePartOfMvPrimaryKey =
        mvPrimaryKeyColumns.stream()
            .map(baseTable::column)
            .filter(c -> !c.isPrimaryKeyComponent())
            .map(SchemaEntity::name)
            .collect(toList());
    checkArgument(
        nonPrimaryKeysThatArePartOfMvPrimaryKey.size() <= 1,
        "Materialized view %s supports only one source non-primary key component but it defined more than one: %s",
        cqlName(indexName),
        nonPrimaryKeysThatArePartOfMvPrimaryKey);

    query.append("CREATE MATERIALIZED VIEW ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    } else if (schemaKeyspace().table(indexName) != null) {
      throw invalid("A table named %s already exists", cqlName(indexName));
    }
    query.append(baseTable.cqlKeyspace()).append('.').append(cqlName(indexName));
    query.append(" AS SELECT ");
    query.append(createColumns.stream().map(Column::cqlName).collect(Collectors.joining(", ")));
    query.append(" FROM ");
    addName(query, baseTable);
    query.append(" WHERE ");
    query.append(
        createColumns.stream()
            .map(c -> c.cqlName() + " IS NOT NULL")
            .collect(Collectors.joining(" AND ")));
    query.append(" ");
    addPrimaryKey(query, createColumns, indexName);
    WithAdder with = new WithAdder(query);
    addClusteringOrder(with, createColumns);
    addComment(with);
    addDefaultTTL(with);

    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private BuiltQuery<?> dropMaterializedView() {
    StringBuilder query = new StringBuilder();
    Keyspace keyspace = schemaKeyspace();
    query.append("DROP MATERIALIZED VIEW ");
    if (ifExists) {
      query.append("IF EXISTS ");
    } else if (keyspace.table(indexName) == null) {
      throw invalid(
          "Materialized view %s.%s does not exists", keyspace.cqlName(), cqlName(indexName));
    }
    query.append(keyspace.cqlName()).append('.').append(cqlName(indexName));
    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private BuiltQuery<?> createType() {
    StringBuilder query = new StringBuilder();
    Keyspace keyspace = schemaKeyspace();
    query.append("CREATE TYPE ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    } else if (keyspace.userDefinedType(type.name()) != null) {
      throw invalid("A type named %s.%s already exists", keyspace.cqlName(), type.cqlName());
    }
    query.append(keyspace.cqlName()).append('.').append(type.cqlName());
    query.append(
        type.columns().stream()
            .map(c -> c.cqlName() + " " + c.type().cqlDefinition())
            .collect(Collectors.joining(", ", " (", ")")));
    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private BuiltQuery<?> dropType() {
    StringBuilder query = new StringBuilder();
    Keyspace keyspace = schemaKeyspace();
    query.append("DROP TYPE ");
    if (ifExists) {
      query.append("IF EXISTS ");
    } else if (keyspace.userDefinedType(type.name()) == null) {
      throw invalid("User type %s.%s does not exists", keyspace.cqlName(), type.cqlName());
    }
    query.append(keyspace.cqlName()).append('.').append(type.cqlName());
    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private BuiltQuery<?> alterType() {
    StringBuilder query = new StringBuilder();
    Keyspace keyspace = schemaKeyspace();
    query.append("ALTER TYPE ");
    query.append(keyspace.cqlName()).append('.').append(type.cqlName());
    assert !addColumns.isEmpty();
    query.append(" ADD (");
    query.append(
        addColumns.stream()
            .map(c -> c.cqlName() + " " + c.type().cqlDefinition())
            .collect(Collectors.joining(", ")));
    query.append(")");
    return new BuiltOther(valueCodec, executor, query.toString());
  }

  private BuiltInsert insertQuery() {
    Table table = schemaTable();
    QueryStringBuilder builder = new QueryStringBuilder(markerIndex);
    builder.append("INSERT INTO").append(table);
    List<Column> columns =
        dmlModifications.stream()
            .map(
                m -> {
                  checkArgument(m.target().fieldName() == null && m.target().mapKey() == null);
                  return table.existingColumn(m.target().columnName());
                })
            .collect(toList());

    List<ValueModifier> regularAndStaticModifiers = new ArrayList<>();
    List<BuiltCondition> where = new ArrayList<>();
    builder.start("(").addAll(columns).end(")");
    builder.append("VALUES");
    builder
        .start("(")
        .addAllWithIdx(
            dmlModifications,
            (modifier, i) -> {
              Column column = columns.get(i);
              Value<?> value = modifier.value();
              builder.append(markerFor(column), value);
              if (column.isPrimaryKeyComponent()) {
                where.add(BuiltCondition.ofModifier(modifier));
              } else {
                regularAndStaticModifiers.add(modifier);
              }
            })
        .end(")");
    if (ifNotExists) {
      builder.append("IF NOT EXISTS");
    }
    addUsingClause(builder);

    return new BuiltInsert(
        table,
        valueCodec,
        executor,
        builder,
        where,
        regularAndStaticModifiers,
        ifNotExists,
        ttl,
        timestamp);
  }

  private void addUsingClause(QueryStringBuilder builder) {
    builder
        .lazyStart("USING", "AND")
        .addIfNotNull(ttl, ttl -> builder.append("TTL").append(markerFor(Column.TTL), ttl))
        .addIfNotNull(
            timestamp,
            timestamp -> builder.append("TIMESTAMP").append(markerFor(Column.TIMESTAMP), timestamp))
        .end();
  }

  private void addModifier(Column column, ValueModifier modifier, QueryStringBuilder builder) {
    builder.append(column);
    ColumnType type = column.type();
    checkArgument(type != null, "Column %s does not have its type set", column.cqlName());
    String fieldName = modifier.target().fieldName();
    Value<?> mapKey = modifier.target().mapKey();
    String valueDescription = column.cqlName();
    ColumnType valueType = type;
    if (fieldName != null) {
      checkArgument(
          type.isUserDefined(),
          "Cannot update field %s of column %s of type %s, it is not a UDT",
          fieldName,
          column.cqlName(),
          type);
      Column field = ((UserDefinedType) type).columnMap().get(fieldName);
      checkArgument(
          field != null,
          "Column %s (of type %s) has no field %s",
          column.cqlName(),
          type,
          fieldName);
      builder.append('.' + fieldName);
      valueDescription = valueDescription + '.' + fieldName;
      valueType = field.type();
    } else if (mapKey != null) {
      checkArgument(
          type.isMap(),
          "Cannot update values of column %s of type %s, it is not a map",
          column.cqlName(),
          type);
      builder.append(
          markerFor(format("key(%s)", column.cqlName()), type.parameters().get(0)), mapKey);
      valueDescription = format("value(%s)", column.cqlName());
      valueType = type.parameters().get(1);
    }
    builder.append(operationStr(modifier.operation()));
    builder.append(markerFor(valueDescription, valueType), modifier.value());
    // Unfortunately, prepend cannot be expressed with a concise operator and we have to add to it
    if (modifier.operation() == Operation.PREPEND) {
      builder.append("+").append(column);
    }
  }

  private String operationStr(Operation operation) {
    switch (operation) {
      case PREPEND: // fallthrough on purpose
      case SET:
        return "=";
      case APPEND: // fallthrough on purpose
      case INCREMENT:
        return "+=";
      case REMOVE:
        return "-=";
      default:
        throw new UnsupportedOperationException();
    }
  }

  private BuiltUpdate updateQuery() {
    Table table = schemaTable();
    QueryStringBuilder builder = new QueryStringBuilder(markerIndex);
    builder.append("UPDATE").append(table);
    addUsingClause(builder);
    builder
        .start("SET")
        .addAll(
            dmlModifications,
            modifier -> {
              Column column = table.existingColumn(modifier.target().columnName());
              addModifier(column, modifier, builder);
            });
    handleDMLWhere(table, builder);
    handleDMLConditions(table, builder);

    return new BuiltUpdate(
        table,
        valueCodec,
        executor,
        builder,
        wheres,
        dmlModifications,
        ifExists,
        ifs,
        ttl,
        timestamp);
  }

  private void handleDMLWhere(Table table, QueryStringBuilder builder) {
    builder
        .lazyStart("WHERE", "AND")
        .addAll(wheres, where -> where.addToBuilder(table, builder, m -> {}))
        .end();
  }

  private void handleDMLConditions(Table table, QueryStringBuilder builder) {
    builder
        .lazyStart("IF", "AND")
        .addIf(ifExists, () -> builder.append("EXISTS"))
        .addAll(ifs, condition -> condition.addToBuilder(table, builder, m -> {}))
        .end();
  }

  private BuiltDelete deleteQuery() {
    Table table = schemaTable();
    QueryStringBuilder builder = new QueryStringBuilder(markerIndex);
    List<Column> deletedColumns = validateColumns(table, selection);
    builder.append("DELETE");
    builder.start().addAll(deletedColumns).end();
    deletedColumns.forEach(c -> dmlModifications.add(ValueModifier.of(c.name(), Value.of(null))));
    builder.append("FROM").append(table);
    if (timestamp != null) {
      builder.append("USING TIMESTAMP").append(markerFor(Column.TIMESTAMP), timestamp);
    }
    handleDMLWhere(table, builder);
    handleDMLConditions(table, builder);

    return new BuiltDelete(
        table, valueCodec, executor, builder, wheres, dmlModifications, ifExists, ifs, timestamp);
  }

  protected BuiltSelect selectQuery() {
    Table table = schemaTable();
    QueryStringBuilder builder = new QueryStringBuilder(markerIndex);
    List<Column> selectedColumns = convertToColumns(table, selection);
    // Using a linked set for the minor convenience of get back the columns in the order they were
    // passed to the builder "in general".
    Set<Column> allSelected = new LinkedHashSet<>(selectedColumns);
    Column wtColumn = null;
    if (writeTimeColumn != null) {
      wtColumn = table.column(writeTimeColumn);
      allSelected.add(wtColumn);
    }
    builder.append("SELECT");
    if (selectedColumns.isEmpty() && writeTimeColumn == null) {
      builder.append("*");
    } else {
      builder
          .start()
          .addAll(selectedColumns)
          .addIfNotNull(
              wtColumn,
              c -> {
                builder.append("WRITETIME(").append(c).append(")");
                if (writeTimeColumnAlias != null) {
                  builder.append("AS").append(cqlName(writeTimeColumnAlias));
                }
              })
          .end();
    }
    builder.append("FROM").append(table);
    List<Value<?>> internalWhereValues = new ArrayList<>();
    List<BindMarker> internalBindMarkers = new ArrayList<>();
    builder
        .lazyStart("WHERE", "AND")
        .addAll(
            wheres,
            where -> {
              where.addToBuilder(table, builder, internalBindMarkers::add);
              where.lhs().value().ifPresent(internalWhereValues::add);
              internalWhereValues.add(where.value());
            })
        .end();

    builder
        .lazyStart("ORDER BY")
        .addAll(
            orders,
            order -> builder.append(order.column()).append(order.order().name().toUpperCase()));

    if (limit != null) {
      BindMarker marker = markerFor("[limit]", Type.Bigint);
      builder.append("LIMIT").append(marker, limit);
      internalBindMarkers.add(marker);
    }
    if (allowFiltering) {
      builder.append("ALLOW FILTERING");
    }

    return new BuiltSelect(
        table,
        valueCodec,
        executor,
        builder,
        allSelected,
        internalWhereValues,
        internalBindMarkers,
        wheres,
        limit);
  }
}
