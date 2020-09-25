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
package io.stargate.db.datastore.query;

import static com.datastax.oss.driver.shaded.guava.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.github.misberner.apcommons.util.AFModifier;
import com.github.misberner.duzzt.annotations.DSLAction;
import com.github.misberner.duzzt.annotations.GenerateEmbeddedDSL;
import com.github.misberner.duzzt.annotations.SubExpr;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.CollectionIndexingType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.MaterializedView;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.SchemaEntity;
import io.stargate.db.schema.SecondaryIndex;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Convenience builder for creating queries. */
@GenerateEmbeddedDSL(
    modifier = AFModifier.DEFAULT,
    autoVarArgs = false,
    name = "QueryBuilder",
    syntax =
        "(<keyspace>|<table>|<insert>|<update>|<delete>|<select>|<index>|<type>) consistencyLevel? (future|prepare|execute)",
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
              "(create table ifNotExists? column+ withComment? (<withVL>|<withEL>)?) | (alter table (((addColumn+)? | (dropColumn+)?) | (withComment? (<withVL>|<withEL>|<withoutVL>|<withoutEL>)?))) | (drop table ifExists?) | (truncate table) | (<renameGraphLabel>)"),
      @SubExpr(name = "type", definedAs = "(create type ifNotExists?)|(drop type ifExists?)"),
      @SubExpr(name = "insert", definedAs = "insertInto value+ ifNotExists? ttl?"),
      @SubExpr(name = "update", definedAs = "update ttl? value+ where+ ifExists?"),
      @SubExpr(name = "delete", definedAs = "delete column* from where+ ifExists?"),
      @SubExpr(
          name = "select",
          definedAs =
              "select star? column* from (where* limit? orderBy*) allowFiltering? withWriteTimeColumn?"),
      @SubExpr(
          name = "index",
          definedAs =
              "(drop ((materializedView|index) ifExists?)) | (create ((materializedView ifNotExists? asSelect star? (column+) from)"
                  + " | (index ifNotExists? on column (indexKeys|indexValues|indexEntries|indexFull|indexingType)?)))"),
      @SubExpr(name = "withVL", definedAs = "withVertexLabel"),
      @SubExpr(
          name = "withEL",
          definedAs = "withEdgeLabel fromVertexLabel fromColumn+ toVertexLabel toColumn+"),
      @SubExpr(name = "withoutVL", definedAs = "withoutVertexLabel"),
      @SubExpr(name = "withoutEL", definedAs = "withoutEdgeLabel"),
      @SubExpr(
          name = "renameGraphLabel",
          definedAs = "alter table rename (vertexLabel|edgeLabel) fromLabel? toLabel"),
    })
public class QueryBuilderImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryBuilderImpl.class);
  private static final String ANONYMOUS_LABEL = "ANONYMOUS_LABEL";
  private final DataStore dataStore;
  private Schema schema;
  private final StringBuilder query = new StringBuilder();
  private final List<Parameter<?>> parameters = new ArrayList<>();
  private final List<Column> columns = new ArrayList<>();
  private final List<Column> addColumns = new ArrayList<>();
  private final List<Column> dropColumns = new ArrayList<>();
  private List<Where<?>> wheres = new ArrayList<>();
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
  private OptionalLong limit = OptionalLong.empty();
  private List<ColumnOrder> orders = new ArrayList<>();
  private boolean allowFiltering = false;
  private String writeTimeColumn;

  private Keyspace keyspace;
  private Table table;
  private Index index;
  private String replication;
  private boolean ifNotExists;
  private boolean ifExists;
  private Optional<Boolean> durableWrites = Optional.empty();
  private String comment;
  private boolean withAdded = false;
  private String vertexLabel;
  private String edgeLabel;
  private String fromVertexLabel;
  private String toVertexLabel;
  private boolean isWithoutGraphLabel = false;
  private List<Column> fromColumns = new ArrayList<>();
  private List<Column> toColumns = new ArrayList<>();
  private boolean isRenameGraphLabel = false;
  private boolean isRenameVertexLabel = false;
  private String fromLabel;
  private String toLabel;
  private UserDefinedType type;
  private Value<Integer> ttl;
  private ConsistencyLevel consistencyLevel;

  public QueryBuilderImpl(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  public QueryBuilderImpl(DataStore dataStore, String keyspace) {
    this(dataStore);
    this.keyspace = Keyspace.reference(keyspace);
  }

  public List<WhereCondition<?>> getConditions() {
    return expression().getAllNodesOfType(WhereCondition.type());
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
    keyspace(Keyspace.reference(keyspace));
  }

  public void keyspace(Keyspace keyspace) {
    this.keyspace = keyspace;
    isKeyspace = true;
  }

  @DSLAction
  public void withVertexLabel() {
    withVertexLabel(ANONYMOUS_LABEL);
  }

  @DSLAction
  public void withEdgeLabel() {
    withEdgeLabel(ANONYMOUS_LABEL);
  }

  @DSLAction
  public void withVertexLabel(String vertexLabel) {
    checkArgument(vertexLabel != null, "'withVertexLabel(label)' must be non-null");
    this.vertexLabel = vertexLabel;
  }

  @DSLAction
  public void withEdgeLabel(String edgeLabel) {
    checkArgument(edgeLabel != null, "'withEdgeLabel(label)' must be non-null");
    this.edgeLabel = edgeLabel;
  }

  @DSLAction
  public void withoutVertexLabel() {
    withoutVertexLabel(ANONYMOUS_LABEL);
  }

  @DSLAction
  public void withoutEdgeLabel() {
    withoutEdgeLabel(ANONYMOUS_LABEL);
  }

  @DSLAction
  public void withoutVertexLabel(String vertexLabel) {
    checkArgument(vertexLabel != null, "'withoutVertexLabel(label)' must be non-null");
    this.vertexLabel = vertexLabel;
    isWithoutGraphLabel = true;
  }

  @DSLAction
  public void withoutEdgeLabel(String edgeLabel) {
    checkArgument(edgeLabel != null, "'withoutEdgeLabel(label)' must be non-null");
    this.edgeLabel = edgeLabel;
    isWithoutGraphLabel = true;
  }

  @DSLAction
  public void fromVertexLabel(String fromVertexLabel) {
    checkArgument(fromVertexLabel != null, "'fromVertexLabel(label)' must be non-null");
    this.fromVertexLabel = fromVertexLabel;
  }

  @DSLAction
  public void toVertexLabel(String toVertexLabel) {
    checkArgument(toVertexLabel != null, "'toVertexLabel(label)' must be non-null");
    this.toVertexLabel = toVertexLabel;
  }

  @DSLAction
  public void rename() {
    this.isRenameGraphLabel = true;
  }

  @DSLAction
  public void vertexLabel() {
    this.isRenameVertexLabel = true;
  }

  @DSLAction
  public void edgeLabel() {
    this.isRenameVertexLabel = false;
  }

  @DSLAction
  public void fromLabel(String fromLabel) {
    this.fromLabel = fromLabel;
  }

  @DSLAction
  public void toLabel(String toLabel) {
    this.toLabel = toLabel;
  }

  @DSLAction
  public void fromColumn(String column) {
    fromColumn(Column.create(column, Column.Kind.Clustering));
  }

  @DSLAction
  public void fromColumn(String column, Column.Kind kind) {
    fromColumn(Column.create(column, kind));
  }

  @DSLAction
  public void fromColumn(Column column) {
    fromColumns.add(column);
  }

  public void fromColumn(List<Column> columns) {
    for (Column column : columns) {
      fromColumn(column);
    }
  }

  @DSLAction
  public void toColumn(String column, Column.Kind kind) {
    toColumn(Column.create(column, kind));
  }

  @DSLAction
  public void toColumn(Column column) {
    toColumns.add(column);
  }

  public void toColumn(List<Column> columns) {
    for (Column column : columns) {
      toColumn(column);
    }
  }

  @DSLAction
  public void table(String keyspace, String table) {
    table(Keyspace.reference(keyspace), Table.reference(keyspace, table));
  }

  @DSLAction
  public void table(String table) {
    checkArgument(keyspace != null, "Keyspace must be specified");
    table(keyspace, Table.reference(keyspace.name(), table));
  }

  public void table(Keyspace keyspace, Table table) {
    this.keyspace = keyspace;
    this.table = table;
    isTable = true;
  }

  @DSLAction
  public void withReplication(String replication) {
    this.replication = replication;
  }

  @DSLAction
  public void andDurableWrites(boolean durableWrites) {
    this.durableWrites = Optional.of(durableWrites);
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
  public void column(String column) {
    column(Column.reference(column));
  }

  public void column(Column column) {
    checkMaterializedViewColumn(column);
    checkTableColumn(column);
    columns.add(column);
  }

  public void column(Collection<Column> column) {
    for (Column c : column) {
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
  public void column(String column, Class type, Column.Kind kind) {
    column(ImmutableColumn.builder().name(column).type(type).kind(kind).build());
  }

  @DSLAction
  public void column(String column, Class type, Column.Kind kind, Column.Order order) {
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

  public void star() {
    column(Column.STAR);
  }

  @DSLAction
  public void column(String column, Column.ColumnType type) {
    column(column, type, Column.Kind.Regular);
  }

  private void checkMaterializedViewColumn(Column column) {
    checkArgument(
        !isCreate || !isMaterializedView || column.type() == null,
        "Column '%s' type should not be specified for materialized view '%s'",
        column.name(),
        index != null ? index.name() : "");
  }

  private void checkTableColumn(Column column) {
    checkArgument(
        !isCreate || !isTable || column.type() != null,
        "Column '%s' type must be specified for table '%s'",
        column.name(),
        table != null ? table.name() : "");
  }

  @DSLAction
  public void column(String column, Class type) {
    column(column, type, Column.Kind.Regular);
  }

  @DSLAction
  public void addColumn(String column, Column.ColumnType type) {
    addColumn(ImmutableColumn.builder().name(column).type(type).kind(Column.Kind.Regular).build());
  }

  public void addColumn(String column) {
    addColumns.add(Column.reference(column));
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
    dropColumn(Column.reference(column));
  }

  public void dropColumn(Column column) {
    dropColumns.add(column);
  }

  public void dropColumn(Collection<Column> columns) {
    for (Column column : columns) {
      dropColumn(column);
    }
  }

  @DSLAction
  public void insertInto(String keyspace, String table) {
    insertInto(Keyspace.reference(keyspace), Table.reference(keyspace, table));
  }

  public void insertInto(Keyspace keyspace, Table table) {
    isInsert = true;
    this.keyspace = keyspace;
    this.table = table;
  }

  @DSLAction
  public void update(String keyspace, String table) {
    update(Keyspace.reference(keyspace), Table.reference(keyspace, table));
  }

  public void update(Keyspace keyspace, Table table) {
    isUpdate = true;
    this.keyspace = keyspace;
    this.table = table;
  }

  @DSLAction
  public void delete() {
    isDelete = true;
  }

  @DSLAction
  public void select() {
    isSelect = true;
  }

  @DSLAction
  public void from(String keyspace, String queryable) {
    from(Keyspace.reference(keyspace), Table.reference(keyspace, queryable));
  }

  @DSLAction
  public void from(String queryable) {
    checkArgument(keyspace != null, "Keyspace must be specified");
    from(keyspace, Table.reference(keyspace.name(), queryable));
  }

  public void from(Keyspace keyspace, Table table) {
    this.keyspace = keyspace;
    this.table = table;
  }

  public void from(Keyspace keyspace, MaterializedView view) {
    this.keyspace = keyspace;
    this.index = view;
  }

  @DSLAction
  public void value(String column, Object value) {
    value(Column.reference(column), value);
  }

  @DSLAction
  public void value(String column) {
    value(Column.reference(column));
  }

  public void value(Column column) {
    value(ImmutableValue.builder().column(column).build());
  }

  public void value(Column column, Object value) {
    value(ImmutableValue.builder().column(column).value(Optional.ofNullable(value)).build());
  }

  public void value(Value<?> value) {
    columns.add(value.column());
    parameters.add(value);
  }

  public void value(Collection<Value<?>> value) {
    for (Value v : value) {
      value(v);
    }
  }

  public void where(Column column, WhereCondition.Predicate predicate, Object value) {
    where(
        ImmutableWhereCondition.builder().column(column).predicate(predicate).value(value).build());
  }

  public void where(Column column, WhereCondition.Predicate predicate) {
    where(ImmutableWhereCondition.builder().column(column).predicate(predicate).build());
  }

  public void where(String column, WhereCondition.Predicate predicate, Object value) {
    where(Column.reference(column), predicate, value);
  }

  public void where(String column, WhereCondition.Predicate predicate) {
    where(Column.reference(column), predicate);
  }

  public void where(Where<?> where) {
    wheres.add(where);
    parameters.addAll((List) where.getAllNodesOfType(Parameter.class));
  }

  @DSLAction(autoVarArgs = false)
  public void where(Collection<? extends Where<?>> where) {
    for (Where<?> w : where) {
      where(w);
    }
  }

  @DSLAction
  public void materializedView(String keyspace, String name) {
    materializedView(Keyspace.reference(keyspace), name);
  }

  @DSLAction
  public void materializedView(String name) {
    checkArgument(keyspace != null, "Keyspace must be specified");
    materializedView(keyspace, name);
  }

  public void materializedView(Keyspace keyspace, String name) {
    this.keyspace = keyspace;
    this.index = MaterializedView.reference(keyspace.name(), name);
    isMaterializedView = true;
  }

  @DSLAction
  public void asSelect() {}

  @DSLAction
  public void on(String keyspace, String table) {
    on(Keyspace.reference(keyspace), Table.reference(keyspace, table));
  }

  @DSLAction
  public void on(String table) {
    checkArgument(keyspace != null, "Keyspace must be specified");
    on(keyspace, Table.reference(keyspace.name(), table));
  }

  public void on(Keyspace keyspace, Table table) {
    this.keyspace = keyspace;
    this.table = table;
  }

  @DSLAction
  public void index(String index) {
    index(SecondaryIndex.reference(index));
  }

  public void index(SecondaryIndex index) {
    this.index = index;
    isIndex = true;
  }

  @DSLAction
  public void index(String keyspace, String index) {
    index(Keyspace.reference(keyspace), SecondaryIndex.reference(index));
  }

  public void index(Keyspace keyspace, SecondaryIndex index) {
    this.keyspace = keyspace;
    this.index = index;
    isIndex = true;
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
  public void type(String keyspace, UserDefinedType userDefinedType) {
    this.type = userDefinedType;
    this.keyspace = Keyspace.reference(keyspace);
    isType = true;
  }

  public void limit(long limit) {
    this.limit = OptionalLong.of(limit);
  }

  public void limit(OptionalLong limit) {
    this.limit = limit;
  }

  public void orderBy(Column column, Column.Order order) {
    orderBy(ColumnOrder.of(column, order));
  }

  public void orderBy(ColumnOrder... orders) {
    Collections.addAll(this.orders, orders);
  }

  public void orderBy(List<ColumnOrder> orders) {
    this.orders = orders;
  }

  public void orderBy(String column, Column.Order order) {
    orderBy(Column.reference(column), order);
  }

  public void orderBy(String column) {
    orderBy(column, Column.Order.Asc);
  }

  public void orderBy(Column column) {
    orderBy(column, Column.Order.Asc);
  }

  public void allowFiltering(boolean allowFiltering) {
    this.allowFiltering = allowFiltering;
  }

  public void withWriteTimeColumn(String columnName) {
    this.writeTimeColumn = columnName;
  }

  public CompletableFuture<ResultSet> future() {
    return prepare().thenCompose(PreparedStatement::execute);
  }

  @DSLAction
  public ResultSet execute(Object... args) throws ExecutionException, InterruptedException {
    return prepare()
        .thenCompose(p -> p.execute(Optional.ofNullable(this.consistencyLevel), args))
        .get();
  }

  @DSLAction
  public void consistencyLevel(ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
  }

  @DSLAction
  public void ttl() {
    ttl = ImmutableValue.<Integer>builder().column(Column.TTL).build();
    parameters.add(this.ttl);
  }

  @DSLAction
  public void ttl(int ttl) {
    this.ttl = ImmutableValue.<Integer>builder().column(Column.TTL).value(ttl).build();
    parameters.add(this.ttl);
  }

  CompletableFuture<MixinPreparedStatement> prepare() {
    return createStatement();
  }

  protected CompletableFuture<MixinPreparedStatement> createStatement() {
    schema = dataStore.schema();
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

    if (isSelect) {
      return selectQuery();
    }
    if (isUpdate) {
      return updateQuery();
    }
    if (isInsert) {
      return insertQuery();
    }
    if (isDelete) {
      return deleteQuery();
    }

    throw new AssertionError("Unknown query type");
  }

  private CompletableFuture<MixinPreparedStatement> createType() {
    query.append("CREATE TYPE ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    } else {
      checkNoType();
    }
    qualifiedName(keyspace, type);

    query.append(
        type.columns().stream()
            .map(c -> c.cqlName() + " " + c.type().cqlDefinition())
            .collect(Collectors.joining(", ", " (", ")")));
    return prepareInternal(query.toString());
  }

  protected CompletableFuture<MixinPreparedStatement> prepareInternal(
      String cql, Optional<Index> index) {
    return dataStore
        .prepare(cql, index)
        .thenApply(prepared -> new MixinPreparedStatement(prepared, parameters));
  }

  protected CompletableFuture<MixinPreparedStatement> prepareInternal(String cql) {
    return prepareInternal(cql, Optional.empty());
  }

  private CompletableFuture<MixinPreparedStatement> dropType() {
    query.append("DROP TYPE ");
    if (ifExists) {
      query.append("IF EXISTS ");
    } else {
      checkType();
    }
    qualifiedName(keyspace, type);
    return prepareInternal(query.toString());
  }

  private void checkNoType() {
    if (type != null) {
      checkKeyspace();
      checkArgument(
          schema.keyspace(keyspace.name()).userDefinedType(type.name()) == null,
          "User defined type '%s.%s' already exists",
          keyspace.name(),
          type.name());
    }
  }

  private void checkType() {
    checkKeyspace();
    checkArgument(
        schema.keyspace(keyspace.name()).userDefinedType(type.name()) != null,
        "Unknown user defined type '%s.%s'",
        keyspace.name(),
        type.name());
  }

  private CompletableFuture<MixinPreparedStatement> dropKeyspace() {
    query.append("DROP KEYSPACE ");
    if (ifExists) {
      query.append("IF EXISTS ");
    } else {
      checkKeyspace();
    }
    query.append(keyspace.cqlName());
    return prepareInternal(query.toString());
  }

  private CompletableFuture<MixinPreparedStatement> alterKeyspace() {
    checkKeyspace();
    query.append("ALTER KEYSPACE ");
    query.append(keyspace.cqlName());

    if (null != replication) {
      with();
      query.append(" REPLICATION = ");
      query.append(replication);
    }

    if (durableWrites.isPresent()) {
      with();
      query.append(" DURABLE_WRITES = ");
      query.append(durableWrites.get().booleanValue());
    }

    return prepareInternal(query.toString());
  }

  private CompletableFuture<MixinPreparedStatement> createKeyspace() {
    query.append("CREATE KEYSPACE ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    } else {
      checkNoKeyspace();
    }
    query.append(keyspace.cqlName());

    with();
    query.append(" REPLICATION = ");
    query.append(replication);

    query.append(" AND DURABLE_WRITES = ");
    query.append((durableWrites.orElse(Boolean.TRUE)).booleanValue());

    return prepareInternal(query.toString());
  }

  private CompletableFuture<MixinPreparedStatement> deleteQuery() {
    checkTable();
    query.append("DELETE ");

    query.append(columns.stream().map(c -> c.cqlName()).collect(Collectors.joining(", ")));
    if (!columns.isEmpty()) {
      query.append(" ");
    }
    query.append("FROM ");
    qualifiedName(keyspace, table);
    query.append(" WHERE ");
    query.append(
        getConditions().stream()
            .map(w -> w.column().cqlName() + " " + w.predicate() + " ?")
            .collect(Collectors.joining(" AND ")));
    if (ifExists) {
      query.append(" IF EXISTS");
    }
    return prepareInternal(query.toString());
  }

  private CompletableFuture<MixinPreparedStatement> insertQuery() {
    checkTable();
    query.append("INSERT INTO ");
    qualifiedName(keyspace, table);
    query.append(" (");
    query.append(columns.stream().map(c -> c.cqlName()).collect(Collectors.joining(", ")));
    query.append(") VALUES (");
    query.append(columns.stream().map(c -> "?").collect(Collectors.joining(", ")));
    query.append(")");
    if (ifNotExists) {
      query.append(" IF NOT EXISTS");
    }
    if (ttl != null) {
      query.append(" USING TTL ?");
    }
    return prepareInternal(query.toString());
  }

  private CompletableFuture<MixinPreparedStatement> updateQuery() {
    checkTable();
    query.append("UPDATE ");
    qualifiedName(keyspace, table);
    if (ttl != null) {
      query.append(" USING TTL ?");
    }
    query.append(" SET ");
    query.append(columns.stream().map(c -> c.cqlName() + " = ?").collect(Collectors.joining(", ")));
    query.append(" WHERE ");
    query.append(
        getConditions().stream()
            .map(w -> w.column().cqlName() + " " + w.predicate() + " ?")
            .collect(Collectors.joining(" AND ")));
    if (ifExists) {
      query.append(" IF EXISTS");
    }
    return prepareInternal(query.toString());
  }

  private int indexSort(Index left, Index right) {
    return Integer.compare(left.priority(), right.priority());
  }

  protected CompletableFuture<MixinPreparedStatement> selectQuery() {
    checkKeyspace();
    setTableOrMaterializedView();
    AbstractTable queryable = getTableOrMaterializedView();
    checkColumns(queryable);
    checkWheres(queryable);
    checkOrders(queryable);
    Optional<Index> selectedIndex = selectIndex(queryable);

    if (queryable instanceof Table && allowFiltering) {
      wheres = filterWheresAndRebuildParameters(new ArrayList<>(wheres), parameters, 0);
      orders = filterOrders(new ArrayList<>(orders), selectedIndex.orElse(queryable));

      // we have to re-select the index because we modified the Wheres
      selectedIndex = selectIndex(queryable);
    }

    return createSelectQuery(table, selectedIndex);
  }

  private Where<?> expression() {
    if (wheres.size() == 1) {
      return wheres.get(0);
    }
    return ImmutableAndWhere.builder().addAllChildren((List) wheres).build();
  }

  private CompletableFuture<MixinPreparedStatement> createSelectQuery(
      Table table, Optional<Index> selectedIndex) {
    AbstractTable queryable =
        selectedIndex
            .map(i -> i instanceof MaterializedView ? ((AbstractTable) i) : table)
            .orElse(table);
    query.append("SELECT ");
    if (columns.contains(Column.STAR) || (columns.isEmpty() && writeTimeColumn == null)) {
      query.append("*");
    } else if (writeTimeColumn != null) {
      query.append(writeTime(writeTimeColumn));
      if (!columns.isEmpty()) {
        query.append(", ");
        query.append(columns.stream().map(Column::cqlName).collect(Collectors.joining(", ")));
      }
    } else {
      query.append(columns.stream().map(Column::cqlName).collect(Collectors.joining(", ")));
    }
    query.append(" FROM ");
    if (queryable != null) {
      qualifiedName(keyspace, queryable);
    }

    boolean hasOrWheresOrSearchPredicates = hasOrWheresOrSearchPredicates(getWheres());
    if (!wheres.isEmpty() && !hasOrWheresOrSearchPredicates) {
      query.append(" WHERE ");
      query.append(
          getConditions().stream().map(this::whereCondition).collect(Collectors.joining(" AND ")));
    }

    if (!orders.isEmpty()) {
      query.append(" ORDER BY ");
      query.append(
          orders.stream()
              .map(order -> order.column().cqlName() + " " + order.order().name().toUpperCase())
              .collect(Collectors.joining(", ")));
    }

    if (limit.isPresent()) {
      query.append(" LIMIT ");
      query.append(limit.getAsLong());
    }

    boolean allowFilteringWithoutIndex =
        allowFiltering && !wheres.isEmpty() && !selectedIndex.isPresent();
    if (allowFilteringWithoutIndex) {
      query.append(" ALLOW FILTERING");
    }

    if (!selectedIndex.isPresent() && !allowFiltering) {
      throw new UnsupportedQueryException(query.toString(), table, expression(), orders);
    }

    if (columns.contains(Column.STAR) && writeTimeColumn != null) {
      throw new UnsupportedQueryException(query.toString(), table, expression(), orders);
    }

    selectedIndex.ifPresent(this::warnWhenIndexNotReady);

    return prepareInternal(query.toString(), selectedIndex);
  }

  private void warnWhenIndexNotReady(Index selectedIndex) {
    /* TODO: Fix?
    String indexNotReadyMsg = null;
    if (selectedIndex instanceof MaterializedView)
    {
        MaterializedView mv = (MaterializedView) selectedIndex;
        if (!dataStore.isMaterializedViewBuilt(mv.keyspace(), mv.name()).blockingGet())
        {
            indexNotReadyMsg = DataStoreUtil.mvIndexNotReadyMessage(mv.keyspace(), mv.name());
        }
    }
    else if (selectedIndex instanceof SecondaryIndex)
    {
        SecondaryIndex secondaryIndex = (SecondaryIndex) selectedIndex;
        if (!dataStore.isSecondaryIndexBuilt(secondaryIndex.keyspace(), secondaryIndex.name()).blockingGet())
        {
            indexNotReadyMsg = DataStoreUtil.secondaryIndexNotReadyMessage(secondaryIndex.keyspace(),
                    secondaryIndex.name());
        }
    }

    if (null != indexNotReadyMsg)
    {
        LOGGER.warn(indexNotReadyMsg);
        if (dataStore instanceof InternalDataStore)
        {
            InternalDataStore store = (InternalDataStore) dataStore;
            if (null != store.getWarningBuffer())
            {
                store.getWarningBuffer().addWarning(indexNotReadyMsg);
            }
        }
    }
     */
  }

  /**
   * Filter search predicates in {@link QueryBuilderImpl#wheres} and rebuild the @param parameters
   *
   * <p>Preserve where conditions that are:
   *
   * <ul>
   *   <li>non-search exclusive predicates
   * </ul>
   *
   * Remove where conditions that are:
   *
   * <ul>
   *   <li>Or where expressions
   *   <li>Any search exclusive predicates
   * </ul>
   *
   * and set the corresponding parameters to ignore
   */
  private List<Where<?>> filterWheresAndRebuildParameters(
      final List<Where<?>> wheres, List<Parameter<?>> parameters, int parameterIndex) {
    List<Where<?>> preservedWheres = new ArrayList<>();
    for (Where<?> w : wheres) {
      if (w instanceof AndWhere) {
        AndWhere andWhere = (AndWhere) w;
        List<Where<?>> andWhereChildrenWheres =
            filterWheresAndRebuildParameters(andWhere.children(), parameters, parameterIndex);
        if (!andWhereChildrenWheres.isEmpty()) {
          preservedWheres.add(
              AndWhere.and(
                  andWhereChildrenWheres.toArray(new Where<?>[andWhereChildrenWheres.size()])));
        }
      } else if (w instanceof OrWhere) {
        OrWhere<?> orWhere = (OrWhere) w;
        List<Parameter> orWhereParams = orWhere.getAllNodesOfType(Parameter.class);
        int orWhereParamsSize = orWhereParams.size();
        for (int i = parameterIndex; i < parameterIndex + orWhereParamsSize; i++) {
          parameters.set(i, parameters.get(i).ignore());
        }
        parameterIndex = parameterIndex + orWhereParamsSize;
      } else {
        WhereCondition whereCondition = (WhereCondition) w;
        preservedWheres.add(whereCondition);
        parameterIndex++;
      }
    }
    return preservedWheres;
  }

  private List<ColumnOrder> filterOrders(final List<ColumnOrder> orders, Index index) {
    if (orders.isEmpty() || null == index || index instanceof SecondaryIndex) {
      return Collections.emptyList();
    }

    Set<String> indexedColumnNames = Collections.emptySet();
    if (index instanceof AbstractTable) {
      indexedColumnNames =
          ((AbstractTable) index)
              .clusteringKeyColumns().stream().map(c -> c.name()).collect(toSet());
    }

    List<ColumnOrder> preservedOrders = new ArrayList<>();
    for (ColumnOrder order : orders) {
      if (indexedColumnNames.contains(order.column().name())) {
        preservedOrders.add(order);
      }
    }
    return preservedOrders;
  }

  private boolean hasOrWheresOrSearchPredicates(List<Where<?>> wheres) {
    for (Where<?> w : wheres) {
      if (w instanceof OrWhere) {
        return true;
      } else if (w instanceof AndWhere) {
        AndWhere andWhere = (AndWhere) w;
        if (hasOrWheresOrSearchPredicates(andWhere.children())) {
          return true;
        }
      }
    }
    return false;
  }

  private String whereCondition(WhereCondition w) {
    if (w.predicate().equals(WhereCondition.Predicate.EntryEq)
        && w.value().isPresent()
        && w.value().get() instanceof Pair) {
      Pair p = (Pair) w.value().get();
      return String.format("%s[%s] %s ?", w.column().cqlName(), p.getValue0(), w.predicate());
    }
    return w.column().cqlName() + " " + w.predicate() + " ?";
  }

  private void checkWheres(AbstractTable queryable) {
    List<String> colNames = queryable.columns().stream().map(c -> c.name()).collect(toList());
    List<String> unknownWhereColNames =
        getConditions().stream()
            .map(w -> w.column().name())
            .filter(n -> !colNames.contains(n))
            .collect(toList());
    checkArgument(
        unknownWhereColNames.isEmpty(),
        "Query contains unknown where columns '%s'",
        unknownWhereColNames);
  }

  private void checkOrders(AbstractTable queryable) {
    List<String> colNames = queryable.columns().stream().map(c -> c.name()).collect(toList());
    List<String> unknownOrderColNames =
        orders.stream()
            .map(o -> o.column().name())
            .filter(n -> !colNames.contains(n))
            .collect(toList());
    checkArgument(
        unknownOrderColNames.isEmpty(),
        "Query contains unknown order columns '%s'",
        unknownOrderColNames);

    Map<Column, Long> frequency =
        orders.stream().collect(Collectors.groupingBy(ColumnOrder::column, counting()));
    frequency.forEach(
        (col, freq) ->
            checkArgument(
                freq == 1, "Query contained more than one order for column '%s'", col.name()));
  }

  private void setTableOrMaterializedView() {
    if (table != null) {
      Table tableFromSchema = schema.keyspace(keyspace.name()).table(table.name());
      if (tableFromSchema == null) {
        index = schema.keyspace(keyspace.name()).materializedView(table.name());
        if (index != null) {
          table = null;
        }
      } else {
        table = tableFromSchema;
      }
    }
  }

  private Optional<Index> selectIndex(AbstractTable queryable) {
    List<Index> indices = new ArrayList<>();
    indices.add(queryable);
    if (queryable instanceof Table) {
      indices.addAll(((Table) queryable).indexes());
    }
    return indices.stream()
        .sorted(this::indexSort)
        .filter(i -> i.supports(columns, getConditions(), orders, limit))
        .findFirst();
  }

  private CompletableFuture<MixinPreparedStatement> dropMaterializedView() {
    checkKeyspace();
    query.append("DROP MATERIALIZED VIEW ");
    if (ifExists) {
      query.append("IF EXISTS ");
    } else {
      checkIndex();
    }

    qualifiedName(keyspace, index);
    return prepareInternal(query.toString());
  }

  private CompletableFuture<MixinPreparedStatement> createMaterializedView() {
    checkColumns();
    checkTable();
    Table sourceTable = schema.keyspace(keyspace.name()).table(this.table.name());
    Set<String> columnNames = columns.stream().map(c -> c.name()).collect(toSet());
    Set<String> sourceTablePkColumnNames =
        sourceTable.primaryKeyColumns().stream().map(c -> c.name()).collect(toSet());
    Sets.SetView<String> missingColumns = Sets.difference(sourceTablePkColumnNames, columnNames);
    checkArgument(
        missingColumns.isEmpty(),
        "Materialized view '%s' primary key was missing components %s",
        index.name(),
        missingColumns);
    List<String> nonPrimaryKeysThatArePartOfMvPrimaryKey =
        columns.stream()
            .filter(c -> c.kind() != null && c.isPrimaryKeyComponent())
            .map(c -> sourceTable.column(c.name()))
            .filter(c -> !c.isPrimaryKeyComponent())
            .map(c -> c.name())
            .collect(toList());
    checkArgument(
        nonPrimaryKeysThatArePartOfMvPrimaryKey.size() <= 1,
        "Materialized view '%s' supports only one source non-primary key component but it defined more than one: %s",
        index.name(),
        nonPrimaryKeysThatArePartOfMvPrimaryKey);

    query.append("CREATE MATERIALIZED VIEW ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    } else {
      checkNoIndex();
    }
    qualifiedName(keyspace, index);
    query.append(" AS SELECT ");
    if (columns.contains(Column.STAR) || columns.isEmpty()) {
      query.append("*");
    } else {
      query.append(columns.stream().map(c -> c.cqlName()).collect(Collectors.joining(", ")));
    }

    query.append(" FROM ");
    qualifiedName(keyspace, this.table);
    query.append(" WHERE ");
    query.append(
        columns.stream()
            .filter(c -> c != Column.STAR)
            .map(c -> c.cqlName() + " IS NOT NULL")
            .collect(Collectors.joining(" AND ")));
    query.append(" ");
    key();
    clusteringOrder();
    comment();
    return prepareInternal(query.toString());
  }

  private void clusteringOrder() {
    if (columns.stream().anyMatch(c -> c.kind() == Column.Kind.Clustering && c.order() != null)) {
      with();
      query.append(" CLUSTERING ORDER BY (");
      query.append(
          columns.stream()
              .filter(c -> c.kind() == Column.Kind.Clustering)
              .map(c -> c.cqlName() + " " + c.order().name().toUpperCase())
              .collect(Collectors.joining(", ")));
      query.append(")");
    }
  }

  private void comment() {
    if (comment != null) {
      with();
      query.append(" COMMENT = '");
      query.append(comment);
      query.append("'");
    }
  }

  private void withGraphLabel() {
    if (!isWithoutGraphLabel) {
      if (null != vertexLabel) {
        with();
        appendVertexLabel();
      } else if (null != edgeLabel) {
        with();
        appendEdgeLabel();
        query.append(" FROM \"").append(fromVertexLabel).append("\"");
        partitionAndClusteringKeys(fromColumns);
        query.append(" TO \"").append(toVertexLabel).append("\"");
        partitionAndClusteringKeys(toColumns);
      }
    }
  }

  private void partitionAndClusteringKeys(List<Column> columns) {
    checkArgument(
        columns.stream().anyMatch(c -> c.kind() == Column.Kind.PartitionKey),
        "At least one partition key must be specified for FROM: '%s' / TO: '%s' vertex labels",
        fromVertexLabel,
        toVertexLabel);
    query.append("((");
    query.append(
        columns.stream()
            .filter(c -> c.kind() == Column.Kind.PartitionKey)
            .map(c -> c.cqlName())
            .collect(Collectors.joining(", ")));
    query.append(")");
    if (columns.stream().anyMatch(c -> c.kind() == Column.Kind.Clustering)) {
      query.append(", ");
    }
    query.append(
        columns.stream()
            .filter(c -> c.kind() == Column.Kind.Clustering)
            .map(c -> c.cqlName())
            .collect(Collectors.joining(", ")));
    query.append(")");
  }

  private void alterGraphLabel() {
    if (isWithoutGraphLabel) {
      withoutGraphLabel();
    } else {
      withGraphLabel();
    }
  }

  private void withoutGraphLabel() {
    query.append(" WITHOUT");
    if (null != vertexLabel) {
      appendVertexLabel();
    } else if (null != edgeLabel) {
      appendEdgeLabel();
    }
  }

  private void appendVertexLabel() {
    query.append(" VERTEX LABEL");
    if (!ANONYMOUS_LABEL.equals(vertexLabel)) {
      query.append(" \"").append(vertexLabel).append("\"");
    }
  }

  private void appendEdgeLabel() {
    query.append(" EDGE LABEL");
    if (!ANONYMOUS_LABEL.equals(edgeLabel)) {
      query.append(" \"").append(edgeLabel).append("\"");
    }
  }

  private CompletableFuture<MixinPreparedStatement> dropIndex() {
    checkKeyspace();
    query.append("DROP INDEX ");
    if (ifExists) {
      query.append("IF EXISTS ");
    } else {
      checkIndex();
    }
    qualifiedName(keyspace, index);
    return prepareInternal(query.toString());
  }

  private CompletableFuture<MixinPreparedStatement> createIndex() {
    checkColumns();
    Column c = columns.get(0);
    checkIndexTypeIfCollection(c);
    query.append("CREATE INDEX ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    } else {
      checkNoIndex();
    }
    query.append(index.cqlName());
    query.append(" ON ");
    qualifiedName(keyspace, table);
    query.append(" (");
    if (indexKeys) {
      query.append("KEYS(");
    } else if (indexValues) {
      query.append("VALUES(");
    } else if (indexEntries) {
      query.append("ENTRIES(");
    } else if (indexFull) {
      query.append("FULL(");
    }
    query.append(c.cqlName());
    if (indexKeys || indexValues || indexEntries || indexFull) {
      query.append(")");
    }
    query.append(")");
    return prepareInternal(query.toString());
  }

  private void checkIndexTypeIfCollection(Column c) {
    Column column = schema.keyspace(keyspace.name()).table(table.name()).column(c.name());
    if (indexKeys) {
      checkArgument(column.ofTypeMap(), "Indexing keys can only be used with a map");
    }

    if (indexEntries) {
      checkArgument(column.ofTypeMap(), "Indexing entries can only be used with a map");
    }

    if (indexFull) {
      checkArgument(
          column.isFrozenCollection(), "Full indexing can only be used with a frozen list/map/set");
    }
  }

  private CompletableFuture<MixinPreparedStatement> dropTable() {
    checkKeyspace();
    query.append("DROP TABLE ");
    if (ifExists) {
      query.append("IF EXISTS ");
    } else {
      checkTable();
    }
    qualifiedName(keyspace, table);
    return prepareInternal(query.toString());
  }

  private CompletableFuture<MixinPreparedStatement> truncateTable() {
    checkKeyspace();
    checkTable();
    query.append("TRUNCATE ");
    qualifiedName(keyspace, table);
    return prepareInternal(query.toString());
  }

  private CompletableFuture<MixinPreparedStatement> alterTable() {
    checkTable();
    query.append("ALTER TABLE ");
    qualifiedName(keyspace, table);
    if (isRenameGraphLabel) {
      return renameGraphLabel();
    } else {
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
        query.append(dropColumns.stream().map(c -> c.cqlName()).collect(Collectors.joining(", ")));
        query.append(")");
      }
      if (comment != null) {
        query.append(" WITH COMMENT = '");
        query.append(comment);
        query.append("'");
      }
      alterGraphLabel();
    }
    return prepareInternal(query.toString());
  }

  private String writeTime(String writeTimeColumnName) {
    return "writetime(" + writeTimeColumnName + ")";
  }

  private CompletableFuture<MixinPreparedStatement> renameGraphLabel() {
    query.append(" RENAME ");
    if (isRenameVertexLabel) {
      query.append("VERTEX LABEL");
    } else {
      query.append("EDGE LABEL");
    }

    if (!Strings.isNullOrEmpty(fromLabel)) {
      query.append(" \"").append(fromLabel).append("\"");
    }
    query.append(" TO \"").append(toLabel).append("\"");
    return prepareInternal(query.toString());
  }

  private CompletableFuture<MixinPreparedStatement> createTable() {
    checkKeyspace();
    query.append("CREATE TABLE ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    } else {
      checkNoTable();
    }
    qualifiedName(keyspace, table);
    query.append(" (");
    query.append(
        columns.stream()
            .map(
                c ->
                    c.cqlName()
                        + " "
                        + c.type().cqlDefinition()
                        + (c.kind() == Column.Kind.Static ? " STATIC" : ""))
            .collect(Collectors.joining(", ")));
    query.append(", ");
    key();
    query.append(")");

    clusteringOrder();
    comment();
    withGraphLabel();
    return prepareInternal(query.toString());
  }

  private void qualifiedName(Keyspace keyspace, SchemaEntity entity) {
    query.append(keyspace.cqlName());
    query.append(".");
    query.append(entity.cqlName());
  }

  private void key() {
    checkArgument(
        columns.stream()
            .anyMatch(c -> c.kind() == Column.Kind.valueOf(Column.Kind.PartitionKey.name())),
        "At least one partition key must be specified for table or materialized view '%s.%s' %s",
        keyspace.name(),
        table != null ? table.name() : index.name(),
        Arrays.deepToString(columns.toArray()));
    query.append("PRIMARY KEY ((");
    query.append(
        columns.stream()
            .filter(c -> c.kind() == Column.Kind.PartitionKey)
            .map(c -> c.cqlName())
            .collect(Collectors.joining(", ")));
    query.append(")");
    if (columns.stream().anyMatch(c -> c.kind() == Column.Kind.Clustering)) {
      query.append(", ");
    }
    query.append(
        columns.stream()
            .filter(c -> c.kind() == Column.Kind.Clustering)
            .map(c -> c.cqlName())
            .collect(Collectors.joining(", ")));
    query.append(")");
  }

  private void checkKeyspace() {
    if (keyspace != null) {
      checkArgument(
          schema.keyspace(keyspace.name()) != null, "Unknown keyspace '%s'", keyspace.name());
    }
  }

  private void checkNoKeyspace() {
    if (keyspace != null) {
      checkArgument(
          schema.keyspace(keyspace.name()) == null,
          "Keyspace '%s' already exists",
          keyspace.name());
    }
  }

  private void checkTable() {
    if (table != null) {
      checkKeyspace();
      checkArgument(
          schema.keyspace(keyspace.name()).table(table.name()) != null,
          "Unknown table '%s.%s'",
          keyspace.name(),
          table.name());
    }
  }

  private void checkNoTable() {
    if (table != null) {
      checkKeyspace();
      checkArgument(
          schema.keyspace(keyspace.name()).table(table.name()) == null,
          "Table '%s.%s' already exists",
          keyspace.name(),
          table.name());
    }
  }

  private void checkIndex() {
    if (index != null) {
      checkKeyspace();
      checkTable();
      checkArgument(
          schema.keyspace(keyspace.name()).tables().stream()
              .flatMap(t -> t.indexes().stream())
              .anyMatch(i -> index.name().equals(i.name())),
          "Unknown index '%s.%s'",
          keyspace.name(),
          index.name());
    }
  }

  private void checkNoIndex() {
    if (index != null) {
      checkKeyspace();
      checkTable();
      checkArgument(
          schema.keyspace(keyspace.name()).tables().stream()
              .flatMap(t -> t.indexes().stream())
              .noneMatch(i -> index.name().equals(i.name())),
          "Index '%s.%s' already exists",
          keyspace.name(),
          index.name());
    }
  }

  private void checkColumns() {
    checkColumns(getTableOrMaterializedView());
  }

  private void checkColumns(AbstractTable queryable) {
    if (!columns.isEmpty()) {
      columns.stream()
          .filter(c -> c != Column.STAR)
          .forEach(
              c -> {
                checkArgument(
                    queryable.column(c.name()) != null,
                    "Unknown column '%s' on '%s.%s'",
                    c.name(),
                    keyspace.name(),
                    queryable.name());
              });
    }
  }

  public AbstractTable getTableOrMaterializedView() {
    AbstractTable table;
    if (this.table != null) {
      checkTable();
      table = schema.keyspace(keyspace.name()).table(this.table.name());
    } else {
      checkIndex();
      table = schema.keyspace(keyspace.name()).materializedView(this.index.name());
    }
    return table;
  }

  private void with() {
    if (!withAdded) {
      query.append(" WITH");
      withAdded = true;
    } else {
      query.append(" AND");
    }
  }

  public List<Parameter<?>> getParameters() {
    return parameters;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public OptionalLong getLimit() {
    return limit;
  }

  public List<ColumnOrder> getOrders() {
    return orders;
  }

  public List<Where<?>> getWheres() {
    return wheres;
  }

  public boolean isAlter() {
    return isAlter;
  }

  public boolean isCreate() {
    return isCreate;
  }

  public boolean isDelete() {
    return isDelete;
  }

  public boolean isDrop() {
    return isDrop;
  }

  public boolean isIndex() {
    return isIndex;
  }

  public boolean isMaterializedView() {
    return isMaterializedView;
  }

  public boolean isTable() {
    return isTable;
  }

  public boolean isKeyspace() {
    return isKeyspace;
  }

  public Table getTable() {
    return table;
  }

  public Index getIndex() {
    return index;
  }

  public List<Column> getAddColumns() {
    return addColumns;
  }

  public List<Column> getDropColumns() {
    return dropColumns;
  }

  public boolean isDDL() {
    return isAlter() || isCreate() || isDrop();
  }

  public Keyspace getKeyspace() {
    return keyspace;
  }
}
