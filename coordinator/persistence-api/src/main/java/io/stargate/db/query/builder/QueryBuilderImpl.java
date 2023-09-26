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

import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.javatuples.Pair;

/** Convenience builder for creating queries. */
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

  private final List<FunctionCall> functionCalls = new ArrayList<>();

  /** The where conditions for a SELECT or UPDATE. */
  private final List<BuiltCondition> wheres = new ArrayList<>();

  /** The IFs conditions for a conditional UPDATE or DELETE. */
  private final List<BuiltCondition> ifs = new ArrayList<>();

  private @Nullable Value<Integer> limit;
  private @Nullable Value<Integer> perPartitionLimit;
  private final List<Column> groupBys = new ArrayList<>();
  private List<ColumnOrder> orders = new ArrayList<>();

  private Replication replication;
  private boolean ifNotExists;
  private boolean ifExists;
  private @Nullable Boolean durableWrites;
  private String comment;
  private Integer defaultTTL;
  private String indexCreateColumn;
  private String customIndexClass;
  private Map<String, String> customIndexOptions;
  private UserDefinedType type;
  private Value<Integer> ttl;
  private Value<Long> timestamp;
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

  public QueryBuilderImpl create() {
    isCreate = true;
    return this;
  }

  public QueryBuilderImpl alter() {
    isAlter = true;
    return this;
  }

  public QueryBuilderImpl drop() {
    isDrop = true;
    return this;
  }

  public QueryBuilderImpl truncate() {
    isTruncate = true;
    return this;
  }

  public QueryBuilderImpl keyspace(String keyspace) {
    this.keyspaceName = keyspace;
    this.isKeyspace = true;
    return this;
  }

  public QueryBuilderImpl table(String keyspace, String table) {
    this.keyspaceName = keyspace;
    table(table);
    return this;
  }

  public QueryBuilderImpl table(String table) {
    checkArgument(keyspaceName != null, "Keyspace must be specified");
    this.tableName = table;
    this.isTable = true;
    return this;
  }

  public QueryBuilderImpl withReplication(Replication replication) {
    this.replication = replication;
    return this;
  }

  public QueryBuilderImpl andDurableWrites(boolean durableWrites) {
    this.durableWrites = durableWrites;
    return this;
  }

  public QueryBuilderImpl ifNotExists() {
    return ifNotExists(true);
  }

  public QueryBuilderImpl ifNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return this;
  }

  public QueryBuilderImpl ifExists() {
    ifExists(true);
    return this;
  }

  public QueryBuilderImpl ifExists(boolean ifExists) {
    this.ifExists = ifExists;
    return this;
  }

  public QueryBuilderImpl withComment(String comment) {
    this.comment = comment;
    return this;
  }

  public QueryBuilderImpl withDefaultTTL(int defaultTTL) {
    this.defaultTTL = defaultTTL;
    return this;
  }

  public QueryBuilderImpl column(String column) {
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
    return this;
  }

  public QueryBuilderImpl column(String... columns) {
    for (String c : columns) {
      column(c);
    }
    return this;
  }

  public QueryBuilderImpl column(Column column) {
    checkMaterializedViewColumn(column);
    checkTableColumn(column);
    if (isCreate && (isTable || isMaterializedView)) {
      createColumns.add(column);
    } else {
      column(column.name());
    }
    return this;
  }

  public QueryBuilderImpl column(Collection<Column> columns) {
    for (Column c : columns) {
      column(c);
    }
    return this;
  }

  public QueryBuilderImpl column(String column, Column.ColumnType type, Column.Kind kind) {
    column(ImmutableColumn.builder().name(column).type(type).kind(kind).build());
    return this;
  }

  public QueryBuilderImpl column(
      String column, Column.ColumnType type, Column.Kind kind, Column.Order order) {
    column(ImmutableColumn.builder().name(column).type(type).kind(kind).order(order).build());
    return this;
  }

  public QueryBuilderImpl column(String column, Class<?> type, Column.Kind kind) {
    column(ImmutableColumn.builder().name(column).type(type).kind(kind).build());
    return this;
  }

  public QueryBuilderImpl column(
      String column, Class<?> type, Column.Kind kind, Column.Order order) {
    column(ImmutableColumn.builder().name(column).type(type).kind(kind).order(order).build());
    return this;
  }

  public QueryBuilderImpl column(String column, Column.Kind kind) {
    column(ImmutableColumn.builder().name(column).kind(kind).build());
    return this;
  }

  public QueryBuilderImpl column(String column, Column.Kind kind, Column.Order order) {
    column(ImmutableColumn.builder().name(column).kind(kind).order(order).build());
    return this;
  }

  public QueryBuilderImpl column(String column, Column.ColumnType type) {
    column(column, type, Column.Kind.Regular);
    return this;
  }

  public QueryBuilderImpl as(String alias) {
    if (functionCalls.isEmpty()) {
      throw new IllegalStateException(
          "The as() method cannot be called without a preceding function call.");
    }
    // the alias is set for the last function call
    FunctionCall functionCall = functionCalls.get(functionCalls.size() - 1);
    functionCall.setAlias(alias);
    return this;
  }

  public QueryBuilderImpl writeTimeColumn(String columnName) {
    functionCalls.add(FunctionCall.writeTime(columnName));
    return this;
  }

  public QueryBuilderImpl writeTimeColumn(Column columnName) {
    writeTimeColumn(columnName.name());
    return this;
  }

  public QueryBuilderImpl count(String columnName) {
    functionCalls.add(FunctionCall.count(columnName));
    return this;
  }

  public QueryBuilderImpl count(Column columnName) {
    count(columnName.name());
    return this;
  }

  public QueryBuilderImpl max(String maxColumnName) {
    functionCalls.add(FunctionCall.max(maxColumnName));
    return this;
  }

  public QueryBuilderImpl max(Column maxColumnName) {
    max(maxColumnName.name());
    return this;
  }

  public QueryBuilderImpl min(String minColumnName) {
    functionCalls.add(FunctionCall.min(minColumnName));
    return this;
  }

  public QueryBuilderImpl min(Column minColumnName) {
    min(minColumnName.name());
    return this;
  }

  public QueryBuilderImpl sum(String sumColumnName) {
    functionCalls.add(FunctionCall.sum(sumColumnName));
    return this;
  }

  public QueryBuilderImpl sum(Column sumColumnName) {
    sum(sumColumnName.name());
    return this;
  }

  public QueryBuilderImpl avg(String avgColumnName) {
    functionCalls.add(FunctionCall.avg(avgColumnName));
    return this;
  }

  public QueryBuilderImpl avg(Column avgColumnName) {
    avg(avgColumnName.name());
    return this;
  }

  public QueryBuilderImpl function(Collection<FunctionCall> calls) {
    functionCalls.addAll(calls);
    return this;
  }

  public QueryBuilderImpl star() {
    // This can be called to be explicit, but is the default when no columns are selected
    checkArgument(this.selection.isEmpty(), "Cannot use * when other columns are selected");
    return this;
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

  public QueryBuilderImpl column(String column, Class<?> type) {
    column(column, type, Column.Kind.Regular);
    return this;
  }

  public QueryBuilderImpl addColumn(String column, Column.ColumnType type) {
    addColumn(ImmutableColumn.builder().name(column).type(type).kind(Column.Kind.Regular).build());
    return this;
  }

  public QueryBuilderImpl addColumn(Column column) {
    addColumns.add(column);
    return this;
  }

  public QueryBuilderImpl addColumn(Collection<Column> columns) {
    for (Column column : columns) {
      addColumn(column);
    }
    return this;
  }

  public QueryBuilderImpl dropColumn(String column) {
    dropColumns.add(column);
    return this;
  }

  public QueryBuilderImpl dropColumn(Collection<String> columns) {
    for (String column : columns) {
      dropColumn(column);
    }
    return this;
  }

  public QueryBuilderImpl dropColumn(Column column) {
    dropColumn(column.name());
    return this;
  }

  public QueryBuilderImpl renameColumn(String from, String to) {
    columnRenames.add(Pair.with(from, to));
    return this;
  }

  public QueryBuilderImpl renameColumn(List<Pair<String, String>> columnRenames) {
    this.columnRenames.addAll(columnRenames);
    return this;
  }

  public QueryBuilderImpl insertInto(String keyspace, String table) {
    this.keyspaceName = keyspace;
    this.tableName = table;
    this.isInsert = true;
    return this;
  }

  public QueryBuilderImpl insertInto(Table table) {
    insertInto(table.keyspace(), table.name());
    return this;
  }

  public QueryBuilderImpl update(String keyspace, String table) {
    this.keyspaceName = keyspace;
    this.tableName = table;
    this.isUpdate = true;
    return this;
  }

  public QueryBuilderImpl update(Table table) {
    update(table.keyspace(), table.name());
    return this;
  }

  public QueryBuilderImpl delete() {
    this.isDelete = true;
    return this;
  }

  public QueryBuilderImpl select() {
    this.isSelect = true;
    return this;
  }

  public QueryBuilderImpl from(String keyspace, String table) {
    this.keyspaceName = keyspace;
    return from(table);
  }

  public QueryBuilderImpl from(String table) {
    checkArgument(keyspaceName != null, "Keyspace must be specified");
    this.tableName = table;
    return this;
  }

  public QueryBuilderImpl from(AbstractTable table) {
    return from(table.keyspace(), table.name());
  }

  public QueryBuilderImpl value(String column, Object value) {
    return value(ValueModifier.set(column, value));
  }

  public QueryBuilderImpl value(String column) {
    return value(ValueModifier.marker(column));
  }

  public QueryBuilderImpl value(Column column) {
    return value(column.name());
  }

  public QueryBuilderImpl value(Column column, Object value) {
    return value(column.name(), value);
  }

  public QueryBuilderImpl value(ValueModifier modifier) {
    preprocessValue(modifier.target().mapKey());
    preprocessValue(modifier.value());
    dmlModifications.add(modifier);
    return this;
  }

  public QueryBuilderImpl value(Collection<ValueModifier> setters) {
    for (ValueModifier setter : setters) {
      value(setter);
    }
    return this;
  }

  public QueryBuilderImpl where(Column column, Predicate predicate, Object value) {
    return where(column.name(), predicate, value);
  }

  public QueryBuilderImpl where(Column column, Predicate predicate) {
    return where(column.name(), predicate);
  }

  public QueryBuilderImpl where(String columnName, Predicate predicate, Object value) {
    return where(BuiltCondition.of(columnName, predicate, value));
  }

  public QueryBuilderImpl where(String columnName, Predicate predicate) {
    return where(BuiltCondition.ofMarker(columnName, predicate));
  }

  public QueryBuilderImpl where(BuiltCondition where) {
    where.lhs().value().ifPresent(this::preprocessValue);
    preprocessValue(where.value());
    wheres.add(where);
    return this;
  }

  public QueryBuilderImpl where(Collection<? extends BuiltCondition> where) {
    for (BuiltCondition condition : where) {
      where(condition);
    }
    return this;
  }

  public QueryBuilderImpl ifs(String columnName, Predicate predicate, Object value) {
    return ifs(BuiltCondition.of(columnName, predicate, value));
  }

  public QueryBuilderImpl ifs(String columnName, Predicate predicate) {
    return ifs(BuiltCondition.ofMarker(columnName, predicate));
  }

  public QueryBuilderImpl ifs(BuiltCondition condition) {
    condition.lhs().value().ifPresent(this::preprocessValue);
    preprocessValue(condition.value());
    ifs.add(condition);
    return this;
  }

  public QueryBuilderImpl ifs(Collection<? extends BuiltCondition> conditions) {
    for (BuiltCondition condition : conditions) {
      ifs(condition);
    }
    return this;
  }

  public QueryBuilderImpl materializedView(String keyspace, String name) {
    this.keyspaceName = keyspace;
    materializedView(name);
    return this;
  }

  public QueryBuilderImpl materializedView(String name) {
    checkArgument(keyspaceName != null, "Keyspace must be specified");
    // Note that we use the index to store the MV name, because the table variable will be used
    // to store the base table name.
    this.indexName = name;
    this.isMaterializedView = true;
    return this;
  }

  public QueryBuilderImpl materializedView(Keyspace keyspace, String name) {
    return materializedView(keyspace.name(), name);
  }

  public QueryBuilderImpl asSelect() {
    // This method is just so the builder flows better
    return this;
  }

  public QueryBuilderImpl on(String keyspace, String table) {
    this.keyspaceName = keyspace;
    return on(table);
  }

  public QueryBuilderImpl on(String table) {
    checkArgument(keyspaceName != null, "Keyspace must be specified");
    this.tableName = table;
    return this;
  }

  public QueryBuilderImpl on(Table table) {
    return on(table.keyspace(), table.name());
  }

  public QueryBuilderImpl index(String index) {
    this.indexName = index;
    this.isIndex = true;
    return this;
  }

  public QueryBuilderImpl index() {
    return index((String) null);
  }

  public QueryBuilderImpl index(SecondaryIndex index) {
    return index(index.name());
  }

  public QueryBuilderImpl index(String keyspace, String index) {
    this.keyspaceName = keyspace;
    return index(index);
  }

  public QueryBuilderImpl index(Keyspace keyspace, SecondaryIndex index) {
    return index(keyspace.name(), index.name());
  }

  public QueryBuilderImpl indexingType(CollectionIndexingType indexingType) {
    if (indexingType.indexEntries()) {
      indexEntries();
    } else if (indexingType.indexFull()) {
      indexFull();
    } else if (indexingType.indexKeys()) {
      indexKeys();
    } else if (indexingType.indexValues()) {
      indexValues();
    }
    return this;
  }

  public QueryBuilderImpl indexKeys() {
    indexKeys = true;
    return this;
  }

  public QueryBuilderImpl indexValues() {
    indexValues = true;
    return this;
  }

  public QueryBuilderImpl indexEntries() {
    indexEntries = true;
    return this;
  }

  public QueryBuilderImpl indexFull() {
    indexFull = true;
    return this;
  }

  public QueryBuilderImpl custom(String customIndexClass) {
    this.customIndexClass = customIndexClass;
    return this;
  }

  public QueryBuilderImpl custom(String customIndexClass, Map<String, String> customIndexOptions) {
    custom(customIndexClass);
    this.customIndexOptions = customIndexOptions;
    return this;
  }

  public QueryBuilderImpl options(Map<String, String> customIndexOptions) {
    this.customIndexOptions = customIndexOptions;
    return this;
  }

  public QueryBuilderImpl type(String keyspace, UserDefinedType type) {
    this.keyspaceName = keyspace;
    this.type = type;
    this.isType = true;
    return this;
  }

  public QueryBuilderImpl limit() {
    this.limit = Value.marker();
    preprocessValue(this.limit);
    return this;
  }

  public QueryBuilderImpl limit(Integer limit) {
    if (limit != null) {
      this.limit = Value.of(limit);
    }
    return this;
  }

  public QueryBuilderImpl perPartitionLimit() {
    this.perPartitionLimit = Value.marker();
    preprocessValue(this.perPartitionLimit);
    return this;
  }

  public QueryBuilderImpl perPartitionLimit(Integer limit) {
    if (limit != null) {
      this.perPartitionLimit = Value.of(limit);
    }
    return this;
  }

  public QueryBuilderImpl groupBy(Column column) {
    groupBys.add(column);
    return this;
  }

  public QueryBuilderImpl groupBy(String name) {
    groupBy(Column.reference(name));
    return this;
  }

  public QueryBuilderImpl groupBy(Iterable<Column> columns) {
    columns.forEach(this::groupBy);
    return this;
  }

  public QueryBuilderImpl groupBy(Column... columns) {
    for (Column column : columns) {
      groupBy(column);
    }
    return this;
  }

  public QueryBuilderImpl orderBy(Column column, Column.Order order) {
    return orderBy(column.name(), order);
  }

  public QueryBuilderImpl orderBy(ColumnOrder... orders) {
    Collections.addAll(this.orders, orders);
    return this;
  }

  public QueryBuilderImpl orderBy(List<ColumnOrder> orders) {
    this.orders = orders;
    return this;
  }

  public QueryBuilderImpl orderBy(String column, Column.Order order) {
    return orderBy(ColumnOrder.of(column, order));
  }

  public QueryBuilderImpl orderBy(String column) {
    return orderBy(column, Column.Order.ASC);
  }

  public QueryBuilderImpl orderBy(Column column) {
    return orderBy(column, Column.Order.ASC);
  }

  public QueryBuilderImpl allowFiltering() {
    this.allowFiltering = true;
    return this;
  }

  public QueryBuilderImpl allowFiltering(boolean allowFiltering) {
    this.allowFiltering = allowFiltering;
    return this;
  }

  public QueryBuilderImpl ttl() {
    this.ttl = Value.marker();
    preprocessValue(this.ttl);
    return this;
  }

  public QueryBuilderImpl ttl(Integer ttl) {
    if (ttl != null) {
      this.ttl = Value.of(ttl);
    }
    return this;
  }

  public QueryBuilderImpl timestamp() {
    this.timestamp = Value.marker();
    preprocessValue(this.timestamp);
    return this;
  }

  public QueryBuilderImpl timestamp(Long timestamp) {
    if (timestamp != null) {
      this.timestamp = Value.of(timestamp);
    }
    return this;
  }

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
      if (!columnRenames.isEmpty()) {
        return renameTypeColumns();
      }
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

  private static IllegalArgumentException invalid(String format, Object... args) {
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

  private AbstractTable tableOrMaterializedView() {
    AbstractTable table = schemaKeyspace().tableOrMaterializedView(tableName);
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
      String quotedComment = Strings.quote(comment);
      with.add().append(" comment = ").append(quotedComment);
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
      if (customIndexOptions != null && !customIndexOptions.isEmpty()) {
        query.append(
            customIndexOptions.entrySet().stream()
                .map(e -> format("'%s': '%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(", ", " WITH OPTIONS = { ", " }")));
      }
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

  private static List<Column> convertToColumns(
      AbstractTable table, Collection<String> columnNames) {
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

  private BuiltQuery<?> renameTypeColumns() {
    StringBuilder query = new StringBuilder();
    Keyspace keyspace = schemaKeyspace();
    query.append("ALTER TYPE ");
    query.append(keyspace.cqlName()).append('.').append(type.cqlName()).append(" ");
    query.append("RENAME ");

    query.append(
        columnRenames.stream()
            .map(n -> n.getValue0() + " TO " + n.getValue1())
            .collect(Collectors.joining(" AND ")));
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
    query.append(" ADD ");
    query.append(
        addColumns.stream()
            .map(c -> c.cqlName() + " " + c.type().cqlDefinition())
            .collect(Collectors.joining(", ")));
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
    AbstractTable table = tableOrMaterializedView();

    QueryStringBuilder builder = new QueryStringBuilder(markerIndex);
    List<Column> selectedColumns = convertToColumns(table, selection);
    // Using a linked set for the minor convenience of get back the columns in the order they were
    // passed to the builder "in general".
    Set<Column> allSelected = new LinkedHashSet<>(selectedColumns);
    // add all columns used in function calls to allSelected
    for (FunctionCall functionCall : functionCalls) {
      allSelected.add(table.column(functionCall.columnName));
    }

    builder.append("SELECT");
    if (selectedColumns.isEmpty() && functionCalls.isEmpty()) {
      builder.append("*");
    } else {
      QueryStringBuilder.ListBuilder listBuilder = builder.start().addAll(selectedColumns);

      for (FunctionCall functionCall : functionCalls) {
        listBuilder.addIfNotNull(
            functionCall.getColumnName(),
            __ -> addFunctionCallWithAliasIfPresent(builder, functionCall));
      }
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

    builder.lazyStart("GROUP BY").addAll(groupBys, builder::append).end();

    builder
        .lazyStart("ORDER BY")
        .addAll(
            orders,
            order -> builder.append(order.column()).append(order.order().name().toUpperCase()));

    if (perPartitionLimit != null) {
      BindMarker marker = markerFor("[per-partition-limit]", Type.Int);
      builder.append("PER PARTITION LIMIT").append(marker, perPartitionLimit);
      internalBindMarkers.add(marker);
    }

    if (limit != null) {
      BindMarker marker = markerFor("[limit]", Type.Int);
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
        limit,
        perPartitionLimit);
  }

  private static void addFunctionCallWithAliasIfPresent(
      QueryStringBuilder builder, FunctionCall functionCall) {

    builder
        .append(functionCall.getFunctionName() + "(")
        .append(cqlName(functionCall.getColumnName()))
        .append(")");
    if (functionCall.getAlias() != null) {
      builder.append("AS").append(cqlName(functionCall.getAlias()));
    }
  }

  public static class FunctionCall {
    final String columnName;
    @Nullable String alias;
    final String functionName;

    private FunctionCall(String columnName, String alias, String functionName) {
      this.columnName = columnName;
      this.alias = alias;
      this.functionName = functionName;
    }

    public static FunctionCall function(String name, String alias, String functionName) {
      return new FunctionCall(name, alias, functionName);
    }

    public static FunctionCall count(String columnName) {
      return count(columnName, null);
    }

    public static FunctionCall count(String columnName, String alias) {
      return function(columnName, alias, "COUNT");
    }

    public static FunctionCall max(String columnName) {
      return max(columnName, null);
    }

    public static FunctionCall max(String columnName, String alias) {
      return function(columnName, alias, "MAX");
    }

    public static FunctionCall min(String columnName) {
      return min(columnName, null);
    }

    public static FunctionCall min(String columnName, String alias) {
      return function(columnName, alias, "MIN");
    }

    public static FunctionCall avg(String columnName) {
      return avg(columnName, null);
    }

    public static FunctionCall avg(String columnName, String alias) {
      return function(columnName, alias, "AVG");
    }

    public static FunctionCall sum(String columnName) {
      return sum(columnName, null);
    }

    public static FunctionCall sum(String columnName, String alias) {
      return function(columnName, alias, "SUM");
    }

    public static FunctionCall ttl(String columnName) {
      return ttl(columnName, null);
    }

    public static FunctionCall ttl(String columnName, String alias) {
      return function(columnName, alias, "TTL");
    }

    public static FunctionCall writeTime(String columnName) {
      return writeTime(columnName, null);
    }

    public static FunctionCall writeTime(String columnName, String alias) {
      return function(columnName, alias, "WRITETIME");
    }

    public FunctionCall setAlias(String alias) {
      this.alias = alias;
      return this;
    }

    public String getColumnName() {
      return columnName;
    }

    public String getFunctionName() {
      return functionName;
    }

    @Nullable
    public String getAlias() {
      return alias;
    }
  }
}
