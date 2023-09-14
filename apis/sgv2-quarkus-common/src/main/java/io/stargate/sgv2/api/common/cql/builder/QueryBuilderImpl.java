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
package io.stargate.sgv2.api.common.cql.builder;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Variable;
import com.bpodgursky.jbool_expressions.util.ExprFactory;
import com.github.misberner.apcommons.util.AFModifier;
import com.github.misberner.duzzt.annotations.DSLAction;
import com.github.misberner.duzzt.annotations.GenerateEmbeddedDSL;
import com.github.misberner.duzzt.annotations.SubExpr;
import io.quarkus.logging.Log;
import io.stargate.bridge.proto.QueryOuterClass.BatchQuery;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.QueryParameters;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.QueryOuterClass.Values;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.cql.ColumnUtils;
import io.stargate.sgv2.api.common.cql.CqlStrings;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Convenience builder for creating queries.
 *
 * <p>The generated CQL string is wrapped into a {@link Query} that can be passed directly to {@link
 * StargateBridge#executeQuery(Query)}. In addition, any time a {@link Value} is passed to the
 * builder (via such methods as {@link #where(Column, Predicate, Object)}, {@link #value(Column,
 * Object)}, etc.), it is automatically replaced with a bind marker, and stored in {@link
 * Query#getValues()}.
 */
@GenerateEmbeddedDSL(
    modifier = AFModifier.DEFAULT,
    autoVarArgs = false,
    name = "QueryBuilder",
    syntax =
        "(<keyspace>|<table>|<insert>|<update>|<delete>|<select>|<index>|<type>|<other>) parameters? (build|buildForBatch)",
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
              "(create type ifNotExists? column+) | (drop type ifExists?) | (alter type (addColumn+ | renameColumn+))"),
      @SubExpr(name = "insert", definedAs = "insertInto value+ ifNotExists? ttl? timestamp?"),
      @SubExpr(name = "update", definedAs = "update ttl? timestamp? value+ where+ ifs* ifExists?"),
      @SubExpr(name = "delete", definedAs = "delete column* from timestamp? where+ ifs* ifExists?"),
      @SubExpr(
          name = "select",
          definedAs =
              "select star? column* function* ((count|min|max|avg|sum|writeTimeColumn|similarityCosine|similarityDotProduct|similarityEuclidean) as?)* "
                  + "from (where* perPartitionLimit? limit? groupBy* orderBy* vsearch*) allowFiltering?"),
      @SubExpr(
          name = "index",
          definedAs =
              "(drop ((materializedView|index) ifExists?)) | (create ((materializedView ifNotExists? asSelect (column+) from withComment?)"
                  + " | (index ifNotExists? on column (indexKeys|indexValues|indexEntries|indexFull|indexingType)? (custom options?)?)))"),
      @SubExpr(name = "other", definedAs = "cql")
    })
@SuppressWarnings("PMD.ExcessiveClassLength")
public class QueryBuilderImpl {

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
  private boolean isOther;
  private CollectionIndexingType indexingType;

  private String keyspaceName;
  private String tableName;
  private String indexName;
  private String typeName;

  private final List<Column> createColumns = new ArrayList<>();

  private final List<Column> addColumns = new ArrayList<>();
  private final List<String> dropColumns = new ArrayList<>();
  private final Map<String, String> columnRenames = new LinkedHashMap<>();

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

  /** The where expression which contains conditions and logic operation for a SELECT or UPDATE. */
  private Expression<BuiltCondition> whereExpression = null;

  /** The IFs conditions for a conditional UPDATE or DELETE. */
  private final List<BuiltCondition> ifs = new ArrayList<>();

  private Term limitTerm;
  private Integer limitInt;
  private Term perPartitionLimitTerm;
  private Integer perPartitionLimitInt;
  private final List<String> groupBys = new ArrayList<>();
  private final Map<String, Column.Order> orders = new LinkedHashMap<>();

  private String orderByAnn;
  // The bind markers that were generated if the client passed Value instances to the API methods
  private final Map<Marker, Value> generatedMarkers = new HashMap<>();
  // The generated bind values in their final order in the query
  private final List<Value> generatedBoundValues = new ArrayList<>();
  // Whether the client created explicit bind markers, e.g. with Term.marker()
  private boolean hasExplicitMarkers;

  private Replication replication;
  private boolean ifNotExists;
  private boolean ifExists;
  private Boolean durableWrites;
  private String comment;
  private Integer defaultTTL;
  private String indexCreateColumn;
  private String customIndexClass;
  private Map<String, String> customIndexOptions;
  private Term ttlTerm;
  private Integer ttlInt;
  private Term timestampTerm;
  private Long timestampLong;
  private boolean allowFiltering;
  private QueryParameters parameters;
  private String cql;

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
  public void cql(String cql) {
    this.isOther = true;
    this.cql = cql;
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
      if (isTable || isType) {
        throw invalid("Column '%s' type must be specified for a table or type creation");
      } else if (isMaterializedView) {
        createColumns.add(Column.reference(column));
      } else if (isIndex) {
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
    if (isCreate) {
      if ((isTable || isType) && column.type() == null) {
        throw invalid("Column '%s' type must be specified for a table or type creation");
      }
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
  public void column(String column, String type, Column.Kind kind) {
    column(ImmutableColumn.builder().name(column).type(type).kind(kind).build());
  }

  @DSLAction
  public void column(String column, String type, Column.Kind kind, Column.Order order) {
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
  public void column(String column, String type) {
    column(column, type, Column.Kind.REGULAR);
  }

  public void as(String alias) {
    if (functionCalls.isEmpty()) {
      throw new IllegalStateException(
          "The as() method cannot be called without a preceding function call.");
    }
    // the alias is set for the last function call
    FunctionCall functionCall = functionCalls.get(functionCalls.size() - 1);
    functionCall.setAlias(alias);
  }

  public void writeTimeColumn(String columnName) {
    functionCalls.add(FunctionCall.writeTime(columnName));
  }

  public void writeTimeColumn(Column columnName) {
    writeTimeColumn(columnName.name());
  }

  public void count(String columnName) {
    functionCalls.add(FunctionCall.count(columnName));
  }

  public void count(Column columnName) {
    count(columnName.name());
  }

  public void count() {
    count((String) null);
  }

  public void max(String maxColumnName) {
    functionCalls.add(FunctionCall.max(maxColumnName));
  }

  public void max(Column maxColumnName) {
    max(maxColumnName.name());
  }

  public void min(String minColumnName) {
    functionCalls.add(FunctionCall.min(minColumnName));
  }

  public void min(Column minColumnName) {
    min(minColumnName.name());
  }

  public void sum(String sumColumnName) {
    functionCalls.add(FunctionCall.sum(sumColumnName));
  }

  public void sum(Column sumColumnName) {
    sum(sumColumnName.name());
  }

  public void avg(String avgColumnName) {
    functionCalls.add(FunctionCall.avg(avgColumnName));
  }

  public void avg(Column avgColumnName) {
    avg(avgColumnName.name());
  }

  public void similarityCosine(String columnName, Value value) {
    functionCalls.add(FunctionCall.similarityCosine(columnName, termFor(value)));
  }

  public void similarityDotProduct(String columnName, Value value) {
    functionCalls.add(FunctionCall.similarityDotProduct(columnName, termFor(value)));
  }

  public void similarityEuclidean(String columnName, Value value) {
    functionCalls.add(FunctionCall.similarityEuclidean(columnName, termFor(value)));
  }

  public void function(Collection<FunctionCall> calls) {
    functionCalls.addAll(calls);
  }

  public void star() {
    if (!this.selection.isEmpty()) {
      throw invalid("Cannot use * when other columns are selected");
    }
  }

  @DSLAction
  public void addColumn(String column, String type) {
    addColumn(ImmutableColumn.builder().name(column).type(type).kind(Column.Kind.REGULAR).build());
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
    columnRenames.put(from, to);
  }

  public void renameColumn(Map<String, String> columnRenames) {
    this.columnRenames.putAll(columnRenames);
  }

  @DSLAction
  public void insertInto(String keyspace, String table) {
    this.keyspaceName = keyspace;
    this.tableName = table;
    this.isInsert = true;
  }

  public void insertInto(String table) {
    insertInto(null, table);
  }

  @DSLAction
  public void update(String keyspace, String table) {
    this.keyspaceName = keyspace;
    this.tableName = table;
    this.isUpdate = true;
  }

  public void update(String table) {
    update(null, table);
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
    this.tableName = table;
  }

  @DSLAction
  public void value(String column, Object value) {
    addModifier(ValueModifier.set(column, termFor(value)));
  }

  public void value(Column column, Object value) {
    value(column.name(), value);
  }

  public void value(ValueModifier modifier) {
    Term newValue = bindGrpcValues(modifier.value());
    ValueModifier.Target newTarget = bindGrpcValues(modifier.target());
    if (newValue != modifier.value() || newTarget != modifier.target()) {
      modifier = ValueModifier.of(newTarget, modifier.operation(), newValue);
    }
    addModifier(modifier);
  }

  private void addModifier(ValueModifier modifier) {
    if (isInsert && (modifier.target().fieldName() != null || modifier.target().mapKey() != null)) {
      throw invalid("Can't reference fields or map elements in INSERT queries");
    }
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
    where(column.name(), predicate, Term.marker());
  }

  public void where(String columnName, Predicate predicate, Object value) {
    where(BuiltCondition.of(columnName, predicate, termFor(value)));
  }

  public void where(BuiltCondition where) {
    addWhere(bindGrpcValues(where));
  }

  private void addWhere(BuiltCondition where) {
    Log.warn("···········addWhere hahaha!!!");
    Log.warn(where.predicate());
    Log.warn(where.value());
    Log.warn(where.lhs());
    Log.warn(where);
    Log.warn("···········");
    wheres.add(where);
  }

  private void addWhereExpression(Expression<BuiltCondition> whereExpression) {
    Log.warn("--------addWhereExpression");
    this.whereExpression = whereExpression;
    Log.warn("--------");
  }

  @DSLAction(autoVarArgs = false)
  public void where(Collection<? extends BuiltCondition> where) {
    for (BuiltCondition condition : where) {
      where(condition);
    }
  }

  @DSLAction(autoVarArgs = false)
  public void where(Expression<BuiltCondition> whereExpression) {
    Set<BuiltCondition> conditions = whereExpression.getAllK();
    for (BuiltCondition condition : conditions) {
      Variable<BuiltCondition> newConditionExpression = Variable.of(bindGrpcValues(condition));
      Map<BuiltCondition, Expression<BuiltCondition>> conditionExpressionMap =
          Collections.singletonMap(condition, newConditionExpression);
      whereExpression =
          whereExpression.replaceVars(conditionExpressionMap, new ExprFactory.Default<>());
    }
    addWhereExpression(whereExpression);
  }

  public void ifs(String columnName, Predicate predicate, Object value) {
    addIf(BuiltCondition.of(columnName, predicate, termFor(value)));
  }

  public void ifs(BuiltCondition condition) {
    addIf(bindGrpcValues(condition));
  }

  private void addIf(BuiltCondition condition) {
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
    // Note that we use the index to store the MV name, because the table variable will be used
    // to store the base table name.
    this.indexName = name;
    this.isMaterializedView = true;
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
    this.tableName = table;
  }

  @DSLAction
  public void index(String index) {
    this.indexName = index;
    this.isIndex = true;
  }

  @DSLAction
  public void index() {
    index(null);
  }

  @DSLAction
  public void index(String keyspace, String index) {
    this.keyspaceName = keyspace;
    index(index);
  }

  @DSLAction
  public void indexingType(CollectionIndexingType indexingType) {
    this.indexingType = indexingType;
  }

  @DSLAction
  public void indexKeys() {
    indexingType(CollectionIndexingType.KEYS);
  }

  @DSLAction
  public void indexValues() {
    indexingType(CollectionIndexingType.VALUES);
  }

  @DSLAction
  public void indexEntries() {
    indexingType(CollectionIndexingType.ENTRIES);
  }

  @DSLAction
  public void indexFull() {
    indexingType(CollectionIndexingType.FULL);
  }

  @DSLAction
  public void custom(String customIndexClass) {
    this.customIndexClass = customIndexClass;
  }

  @DSLAction
  public void custom(String customIndexClass, Map<String, String> customIndexOptions) {
    custom(customIndexClass);
    this.customIndexOptions = customIndexOptions;
  }

  @DSLAction
  public void options(Map<String, String> customIndexOptions) {
    this.customIndexOptions = customIndexOptions;
  }

  @DSLAction
  public void type(String keyspace, String typeName) {
    this.keyspaceName = keyspace;
    this.typeName = typeName;
    this.isType = true;
  }

  @DSLAction
  public void limit(Integer limit) {
    this.limitInt = limit;
    this.limitTerm = null;
  }

  @DSLAction
  public void limit(Value limit) {
    this.limitTerm = limit == null ? null : termFor(limit);
    this.limitInt = null;
  }

  @DSLAction
  public void limit() {
    this.limitTerm = termFor(Term.marker());
    this.limitInt = null;
  }

  @DSLAction
  public void perPartitionLimit(Integer perPartitionLimit) {
    this.perPartitionLimitInt = perPartitionLimit;
    this.perPartitionLimitTerm = null;
  }

  @DSLAction
  public void perPartitionLimit(Value perPartitionLimit) {
    this.perPartitionLimitTerm = perPartitionLimit == null ? null : termFor(perPartitionLimit);
    this.perPartitionLimitInt = null;
  }

  @DSLAction
  public void perPartitionLimit() {
    this.perPartitionLimitTerm = termFor(Term.marker());
    this.perPartitionLimitInt = null;
  }

  @DSLAction
  public void groupBy(String name) {
    groupBys.add(name);
  }

  @DSLAction
  public void groupBy(Iterable<String> columns) {
    columns.forEach(this::groupBy);
  }

  public void orderBy(Column column, Column.Order order) {
    orderBy(column.name(), order);
  }

  public void orderBy(String column, Column.Order order) {
    this.orders.put(column, order);
    orderByAnn = null;
  }

  public void vsearch(String column) {
    this.orderByAnn = column;
    this.orders.clear();
  }

  public void orderBy(Map<String, Column.Order> orders) {
    this.orders.clear();
    this.orders.putAll(orders);
    this.orderByAnn = null;
  }

  public void allowFiltering() {
    this.allowFiltering = true;
  }

  public void allowFiltering(boolean allowFiltering) {
    this.allowFiltering = allowFiltering;
  }

  @DSLAction
  public void ttl(Integer ttl) {
    this.ttlInt = ttl;
    this.ttlTerm = null;
  }

  @DSLAction
  public void ttl(Value ttl) {
    this.ttlTerm = ttl == null ? null : termFor(ttl);
    this.ttlInt = null;
  }

  @DSLAction
  public void ttl() {
    this.ttlTerm = termFor(Term.marker());
    this.ttlInt = null;
  }

  @DSLAction
  public void timestamp(Long timestamp) {
    this.timestampLong = timestamp;
    this.timestampTerm = null;
  }

  @DSLAction
  public void timestamp(Value timestamp) {
    this.timestampTerm = timestamp == null ? null : termFor(timestamp);
    this.timestampLong = null;
  }

  @DSLAction
  public void timestamp() {
    this.timestampTerm = termFor(Term.marker());
    this.timestampLong = null;
  }

  @DSLAction
  public void parameters(QueryParameters parameters) {
    this.parameters = parameters;
  }

  @DSLAction
  public Query build() {
    Log.error("begin building sql ");
    Query.Builder query = Query.newBuilder().setCql(buildCql());
    Log.error("begin building sql - query " + query);
    if (!generatedBoundValues.isEmpty()) {
      Log.error("begin building sql - not empy");
      Log.error("begin building sql - not empy " + generatedBoundValues);
      query.setValues(Values.newBuilder().addAllValues(generatedBoundValues).build());
    }
    if (parameters != null) {
      Log.error("pa " + parameters);
      query.setParameters(parameters);
    }
    return query.build();
  }

  @DSLAction
  public BatchQuery buildForBatch() {
    if (parameters != null) {
      throw new IllegalStateException("Parameters aren't supported for a batched query");
    }
    BatchQuery.Builder query = BatchQuery.newBuilder().setCql(buildCql());
    if (!generatedBoundValues.isEmpty()) {
      query.setValues(Values.newBuilder().addAllValues(generatedBoundValues).build());
    }
    return query.build();
  }

  private String buildCql() {
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
      Log.error("go select");
      return selectQuery();
    }
    if (isOther) {
      return cql;
    }

    throw new AssertionError("Unknown query type");
  }

  private static IllegalArgumentException invalid(String format, Object... args) {
    return new IllegalArgumentException(String.format(format, args));
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

  private String createKeyspace() {
    StringBuilder query = new StringBuilder("CREATE KEYSPACE ");
    String ksName = cqlName(keyspaceName);
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    }
    query.append(ksName);

    query.append(" WITH replication = ").append(replication);
    if (durableWrites != null) {
      query.append(" AND durable_writes = ").append(durableWrites);
    }

    return query.toString();
  }

  private String alterKeyspace() {
    StringBuilder query = new StringBuilder("ALTER KEYSPACE ").append(cqlName(keyspaceName));

    WithAdder with = new WithAdder(query);
    if (replication != null) {
      with.add().append(" replication = ").append(replication);
    }
    if (durableWrites != null) {
      with.add().append(" durable_writes = ").append(durableWrites);
    }

    return query.toString();
  }

  private String dropKeyspace() {
    StringBuilder query = new StringBuilder("DROP KEYSPACE ");
    if (ifExists) {
      query.append("IF EXISTS ");
    }
    query.append(cqlName(keyspaceName));

    return query.toString();
  }

  private void addPrimaryKey(StringBuilder query, List<Column> columns, String name) {
    if (columns.stream()
        .noneMatch(c -> c.kind() == Column.Kind.valueOf(Column.Kind.PARTITION_KEY.name()))) {
      throw invalid(
          "At least one partition key must be specified for table or materialized view '%s' %s",
          name, Arrays.deepToString(columns.toArray()));
    }
    query
        .append("PRIMARY KEY (")
        .append(
            columns.stream()
                .filter(c -> c.kind() == Column.Kind.PARTITION_KEY)
                .map(Column::cqlName)
                .collect(Collectors.joining(", ", "(", ")")));
    if (columns.stream().anyMatch(c -> c.kind() == Column.Kind.CLUSTERING)) {
      query.append(", ");
    }
    query
        .append(
            columns.stream()
                .filter(c -> c.kind() == Column.Kind.CLUSTERING)
                .map(Column::cqlName)
                .collect(Collectors.joining(", ")))
        .append(")");
  }

  private void addClusteringOrder(WithAdder with, List<Column> columns) {
    if (columns.stream().anyMatch(c -> c.kind() == Column.Kind.CLUSTERING && c.order() != null)) {
      StringBuilder query = with.add();
      query.append(
          columns.stream()
              .filter(c -> c.kind() == Column.Kind.CLUSTERING)
              .map(c -> c.cqlName() + " " + c.order().name().toUpperCase())
              .collect(Collectors.joining(", ", " CLUSTERING ORDER BY (", ")")));
    }
  }

  private void addComment(WithAdder with) {
    if (comment != null) {
      String quotedComment = CqlStrings.quote(comment);
      with.add().append(" comment = ").append(quotedComment);
    }
  }

  private void addDefaultTTL(WithAdder with) {
    if (defaultTTL != null) {
      with.add().append(" default_time_to_live = ").append(defaultTTL);
    }
  }

  private String maybeQualify(String elementName) {
    if (keyspaceName == null) {
      return cqlName(elementName);
    } else {
      return cqlName(keyspaceName) + '.' + cqlName(elementName);
    }
  }

  private String createTable() {
    StringBuilder query = new StringBuilder("CREATE TABLE ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    }
    query
        .append(maybeQualify(tableName))
        .append(" (")
        .append(
            createColumns.stream()
                .map(
                    c ->
                        c.cqlName()
                            + " "
                            + CqlStrings.doubleQuoteUdts(c.type())
                            + (c.kind() == Column.Kind.STATIC ? " STATIC" : ""))
                .collect(Collectors.joining(", ")))
        .append(", ");
    addPrimaryKey(query, createColumns, tableName);
    query.append(")");

    WithAdder with = new WithAdder(query);
    addClusteringOrder(with, createColumns);
    addComment(with);
    addDefaultTTL(with);

    return query.toString();
  }

  private String alterTable() {
    StringBuilder query = new StringBuilder("ALTER TABLE ").append(maybeQualify(tableName));

    if (!addColumns.isEmpty()) {
      query.append(
          addColumns.stream()
              .map(
                  c ->
                      c.cqlName()
                          + " "
                          + CqlStrings.doubleQuoteUdts(c.type())
                          + (c.kind() == Column.Kind.STATIC ? " STATIC" : ""))
              .collect(Collectors.joining(", ", " ADD (", ")")));
    }
    if (!dropColumns.isEmpty()) {
      query.append(
          dropColumns.stream()
              .map(QueryBuilderImpl::cqlName)
              .collect(Collectors.joining(", ", " DROP (", ")")));
    }
    if (!columnRenames.isEmpty()) {
      query.append(
          columnRenames.entrySet().stream()
              .map(rename -> cqlName(rename.getKey()) + " TO " + cqlName(rename.getValue()))
              .collect(Collectors.joining(" AND ", " RENAME ", "")));
    }
    WithAdder with = new WithAdder(query);
    addComment(with);
    addDefaultTTL(with);
    return query.toString();
  }

  private String dropTable() {
    StringBuilder query = new StringBuilder("DROP TABLE ");
    if (ifExists) {
      query.append("IF EXISTS ");
    }
    query.append(maybeQualify(tableName));
    return query.toString();
  }

  private String truncateTable() {
    return "TRUNCATE " + maybeQualify(tableName);
  }

  private String createIndex() {
    StringBuilder query = new StringBuilder("CREATE");
    if (customIndexClass != null) {
      query.append(" CUSTOM");
    }
    query.append(" INDEX");
    if (ifNotExists) {
      query.append(" IF NOT EXISTS");
    }
    if (indexName != null) {
      query.append(" ").append(cqlName(indexName));
    }
    query.append(" ON ").append(maybeQualify(tableName)).append(" (");
    if (indexingType == null) {
      query.append(cqlName(indexCreateColumn));
    } else {
      switch (indexingType) {
        case KEYS:
          query.append("KEYS(");
          break;
        case VALUES:
          query.append("VALUES(");
          break;
        case ENTRIES:
          query.append("ENTRIES(");
          break;
        case FULL:
          query.append("FULL(");
          break;
        default:
          throw new AssertionError("Unhandled indexing type " + indexingType);
      }
      query.append(cqlName(indexCreateColumn)).append(")");
    }
    query.append(")");
    if (customIndexClass != null) {
      query.append(" USING").append(String.format(" '%s'", customIndexClass));
      if (customIndexOptions != null && !customIndexOptions.isEmpty()) {
        query.append(
            customIndexOptions.entrySet().stream()
                .map(e -> String.format("'%s': '%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(", ", " WITH OPTIONS = { ", " }")));
      }
    }
    return query.toString();
  }

  private String dropIndex() {
    StringBuilder query = new StringBuilder("DROP INDEX ");
    if (ifExists) {
      query.append("IF EXISTS ");
    }
    query.append(maybeQualify(indexName));
    return query.toString();
  }

  private String createMaterializedView() {
    StringBuilder query = new StringBuilder("CREATE MATERIALIZED VIEW ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    }
    query
        .append(maybeQualify(indexName))
        .append(" AS SELECT ")
        .append(createColumns.stream().map(Column::cqlName).collect(Collectors.joining(", ")))
        .append(" FROM ")
        .append(maybeQualify(tableName))
        .append(" WHERE ")
        .append(
            createColumns.stream()
                .map(c -> c.cqlName() + " IS NOT NULL")
                .collect(Collectors.joining(" AND ")))
        .append(" ");
    addPrimaryKey(query, createColumns, indexName);
    WithAdder with = new WithAdder(query);
    addClusteringOrder(with, createColumns);
    addComment(with);
    addDefaultTTL(with);

    return query.toString();
  }

  private String dropMaterializedView() {
    StringBuilder query = new StringBuilder("DROP MATERIALIZED VIEW ");
    if (ifExists) {
      query.append("IF EXISTS ");
    }
    query.append(maybeQualify(indexName));
    return query.toString();
  }

  private String createType() {
    StringBuilder query = new StringBuilder("CREATE TYPE ");
    if (ifNotExists) {
      query.append("IF NOT EXISTS ");
    }
    query
        .append(maybeQualify(typeName))
        .append(
            createColumns.stream()
                .map(c -> c.cqlName() + " " + CqlStrings.doubleQuoteUdts(c.type()))
                .collect(Collectors.joining(", ", " (", ")")));
    return query.toString();
  }

  private String renameTypeColumns() {
    return "ALTER TYPE "
        + maybeQualify(typeName)
        + " RENAME "
        + columnRenames.entrySet().stream()
            .map(e -> e.getKey() + " TO " + e.getValue())
            .collect(Collectors.joining(" AND "));
  }

  private String dropType() {
    StringBuilder query = new StringBuilder("DROP TYPE ");
    if (ifExists) {
      query.append("IF EXISTS ");
    }
    query.append(maybeQualify(typeName));
    return query.toString();
  }

  private String alterType() {
    assert !addColumns.isEmpty();
    return "ALTER TYPE "
        + maybeQualify(typeName)
        + " ADD "
        + addColumns.stream()
            .map(c -> c.cqlName() + " " + CqlStrings.doubleQuoteUdts(c.type()))
            .collect(Collectors.joining(", "));
  }

  private String insertQuery() {
    StringBuilder query =
        new StringBuilder("INSERT INTO ")
            .append(maybeQualify(tableName))
            .append(" (")
            .append(
                dmlModifications.stream()
                    .map(m -> cqlName(m.target().columnName()))
                    .collect(Collectors.joining(", ")))
            .append(") VALUES (")
            .append(
                dmlModifications.stream()
                    .map(m -> formatValue(m.value()))
                    .collect(Collectors.joining(", ")))
            .append(")");
    if (ifNotExists) {
      query.append(" IF NOT EXISTS");
    }
    addUsingClause(query);

    return query.toString();
  }

  private String formatValue(Term value) {
    return formatValue(value, generatedMarkers, generatedBoundValues);
  }

  static String formatValue(Term value, Map<Marker, Value> markers, List<Value> boundValues) {

    // At this stage (query construction), we should only encounter markers: either the client
    // used explicit markers, or they used QueryOuterClass.Value instances and we converted them
    // into markers.
    if (!(value instanceof Marker)) {
      throw new AssertionError("Unexpected literal value");
    }

    Marker marker = (Marker) value;
    Value boundValue = markers.get(marker);
    if (boundValue == null) {
      // The query only uses explicit markers
      assert markers.isEmpty();
    } else {
      // Track the exact order in which values are to be bound to the query
      boundValues.add(boundValue);
    }
    return marker.asCql();
  }

  private void addUsingClause(StringBuilder builder) {
    String prefix = " USING ";
    if (ttlInt != null) {
      builder.append(prefix).append("TTL ").append(ttlInt);
      prefix = " AND ";
    } else if (ttlTerm != null) {
      builder.append(prefix).append("TTL ").append(formatValue(ttlTerm));
      prefix = " AND ";
    }

    if (timestampLong != null) {
      builder.append(prefix).append("TIMESTAMP ").append(timestampLong);
    } else if (timestampTerm != null) {
      builder.append(prefix).append("TIMESTAMP ").append(formatValue(timestampTerm));
    }
  }

  private String formatModifier(ValueModifier modifier) {
    StringBuilder builder = new StringBuilder();

    String columnName = modifier.target().columnName();
    String fieldName = modifier.target().fieldName();
    Term mapKey = modifier.target().mapKey();

    String targetString;
    if (fieldName != null) {
      targetString = cqlName(columnName) + '.' + cqlName(fieldName);
    } else if (mapKey != null) {
      targetString = cqlName(columnName) + '[' + formatValue(mapKey) + ']';
    } else {
      targetString = cqlName(columnName);
    }

    builder
        .append(targetString)
        .append(" ")
        .append(operationStr(modifier.operation()))
        .append(" ")
        .append(formatValue(modifier.value()));
    // Unfortunately, prepend cannot be expressed with a concise operator and we have to add to it
    if (modifier.operation() == ValueModifier.Operation.PREPEND) {
      builder.append(" + ").append(targetString);
    }

    return builder.toString();
  }

  private String operationStr(ValueModifier.Operation operation) {
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

  private String updateQuery() {
    StringBuilder builder = new StringBuilder("UPDATE ").append(maybeQualify(tableName));
    addUsingClause(builder);
    builder
        .append(" SET ")
        .append(
            dmlModifications.stream().map(this::formatModifier).collect(Collectors.joining(", ")));
    appendWheres(builder);
    appendIfs(builder);
    return builder.toString();
  }

  private void appendWheres(StringBuilder builder) {
    if (this.whereExpression != null) {
      appendConditions(this.whereExpression, " WHERE ", builder);
      return;
    }
    appendConditions(this.wheres, " WHERE ", builder);
  }

  private void appendIfs(StringBuilder builder) {
    appendConditions(this.ifs, " IF ", builder);
  }

  private void appendConditions(
      List<BuiltCondition> conditions, String initialPrefix, StringBuilder builder) {

    String prefix = initialPrefix;
    if (initialPrefix.contains("IF") && ifExists) {
      builder.append(prefix).append("EXISTS");
      prefix = " AND ";
    }

    for (BuiltCondition condition : conditions) {
      builder.append(prefix);
      Log.error("?? " + generatedBoundValues);
      Log.error("builder " + builder);
      Log.info("lhs " + condition.lhs());
      Log.info("lhs " + (condition.lhs() instanceof BuiltCondition.LHS.ColumnName));
      Log.error("?? " + generatedBoundValues);
      condition.lhs().appendToBuilder(builder, generatedMarkers, generatedBoundValues);
      Log.error("?? " + generatedBoundValues);

      Log.error("builder " + builder);
      builder
          .append(" ")
          .append(condition.predicate().toString())
          .append(" ")
          .append(formatValue(condition.value()));
      Log.error("builder " + generatedBoundValues);
      Log.error("builder " + builder);
      prefix = " AND ";
    }
  }

  private void appendConditions(
      Expression<BuiltCondition> whereExpression, String initialPrefix, StringBuilder builder) {
    String prefix = initialPrefix;
    if (initialPrefix.contains("IF") && ifExists) {
      builder.append(prefix).append("EXISTS");
      prefix = " AND ";
    }
    builder.append(initialPrefix); // must have where
    Log.error("woqu11 " + builder);
    builder.append(addExpressionCql(whereExpression));
    Log.error("woqu " + builder);
  }

  private StringBuilder addExpressionCql(Expression<BuiltCondition> outterExpression) {
    StringBuilder sb = new StringBuilder();
    List<Expression<BuiltCondition>> innerExpressions = outterExpression.getChildren();
    switch (outterExpression.getExprType()) {
      case "and" -> {
        sb.append(" ( ");
        for (int i = 0; i < innerExpressions.size(); i++) {
          sb.append(addExpressionCql(innerExpressions.get(i)));
          if (i == innerExpressions.size() - 1) {
            break;
          }
          sb.append(" AND ");
        }
        sb.append(" ) ");
        return sb;
      }
      case "or" -> {
        sb.append(" ( ");
        for (int i = 0; i < innerExpressions.size(); i++) {
          sb.append(addExpressionCql(innerExpressions.get(i)));
          if (i == innerExpressions.size() - 1) {
            break;
          }
          sb.append(" OR ");
        }
        sb.append(" ) ");
        return sb;
      }
      case "variable" -> {
        Variable<BuiltCondition> variable = (Variable) outterExpression;
        BuiltCondition condition = variable.getValue();
        condition.lhs().appendToBuilder(sb, generatedMarkers, generatedBoundValues);
        Log.error("woqu " + sb);
        sb.append(" ")
            .append(condition.predicate().toString())
            .append(" ")
            .append(formatValue(condition.value()));
        Log.error("woqu " + sb);

        return sb;
      }
      default -> throw new IllegalArgumentException("Unsupported expression type");
    }
  }

  private String deleteQuery() {
    StringBuilder builder = new StringBuilder("DELETE");
    if (!selection.isEmpty()) {
      builder.append(
          selection.stream()
              .map(QueryBuilderImpl::cqlName)
              .collect(Collectors.joining(", ", " ", "")));
    }
    builder.append(" FROM ").append(maybeQualify(tableName));
    addUsingClause(builder);
    appendWheres(builder);
    appendIfs(builder);
    return builder.toString();
  }

  protected String selectQuery() {
    StringBuilder builder = new StringBuilder("SELECT ");
    if (selection.isEmpty() && functionCalls.isEmpty()) {
      builder.append('*');
    } else {
      builder.append(
          Stream.concat(
                  selection.stream().map(QueryBuilderImpl::cqlName),
                  functionCalls.stream().map(functionCall -> formatFunctionCall(functionCall)))
              .collect(Collectors.joining(", ")));
    }
    builder.append(" FROM ").append(maybeQualify(tableName));

    appendWheres(builder);
    Log.error("after append where " + generatedBoundValues + " " + generatedMarkers);
    if (!groupBys.isEmpty()) {
      builder
          .append(" GROUP BY ")
          .append(
              groupBys.stream().map(QueryBuilderImpl::cqlName).collect(Collectors.joining(", ")));
    }

    if (!orders.isEmpty()) {
      builder
          .append(" ORDER BY ")
          .append(
              orders.entrySet().stream()
                  .map(e -> cqlName(e.getKey()) + " " + e.getValue().name())
                  .collect(Collectors.joining(", ")));
    }

    if (orderByAnn != null) {
      builder.append(" ORDER BY ").append(orderByAnn).append(" ANN OF ?");
    }

    if (perPartitionLimitInt != null) {
      builder.append(" PER PARTITION LIMIT ").append(perPartitionLimitInt);
    } else if (perPartitionLimitTerm != null) {
      builder.append(" PER PARTITION LIMIT ").append(formatValue(perPartitionLimitTerm));
    }

    if (limitInt != null) {
      builder.append(" LIMIT ").append(limitInt);
    } else if (limitTerm != null) {
      builder.append(" LIMIT ").append(formatValue(limitTerm));
    }

    if (allowFiltering) {
      builder.append(" ALLOW FILTERING");
    }
    Log.error("end of selectQuery " + generatedBoundValues + " " + generatedMarkers);
    Log.error("end of selectQuery " + builder);
    return builder.toString();
  }

  private Term termFor(Object value) {
    Objects.requireNonNull(value, Literal.NULL_ERROR_MESSAGE);

    if (value instanceof Marker) {
      if (!generatedMarkers.isEmpty()) {
        throw mixedMarkersError();
      }
      hasExplicitMarkers = true;
      return ((Marker) value);
    } else if (value instanceof Value) {
      if (hasExplicitMarkers) {
        throw mixedMarkersError();
      }
      Marker marker = new Marker();
      generatedMarkers.put(marker, (Value) value);
      return marker;
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported type %s. Use a QueryOuterClass.Value or Term.marker()",
              value.getClass().getSimpleName()));
    }
  }

  private IllegalStateException mixedMarkersError() {
    return new IllegalStateException(
        "Can't have both QueryOuterClass.Value arguments and explicit bind markers");
  }

  /**
   * Inspects a client-provided term to check if it references a gRPC {@link Value}. If so, the
   * value is automatically bound and a bind marker is returned. If not, the instance is returned
   * unchanged.
   */
  private Term bindGrpcValues(Term t) {
    if (!(t instanceof Literal)) {
      return t;
    }
    Value v = ((Literal) t).get();
    return termFor(v);
  }

  /** @see #bindGrpcValues(Term) */
  private ValueModifier.Target bindGrpcValues(ValueModifier.Target t) {
    Term mapKey = t.mapKey();
    if (mapKey == null) {
      return t;
    }
    if (!(mapKey instanceof Literal)) {
      return t;
    }
    Value v = ((Literal) mapKey).get();
    return ValueModifier.Target.mapValue(t.columnName(), termFor(v));
  }

  /** @see #bindGrpcValues(Term) */
  private BuiltCondition.LHS bindGrpcValues(BuiltCondition.LHS lhs) {
    return lhs.value()
        .filter(v -> v instanceof Literal)
        .map(
            v -> {
              Term newValue = termFor(((Literal) v).get());
              return (BuiltCondition.LHS)
                  new BuiltCondition.LHS.MapElement(lhs.columnName(), newValue);
            })
        .orElse(lhs);
  }

  /** @see #bindGrpcValues(Term) */
  private BuiltCondition bindGrpcValues(BuiltCondition where) {
    BuiltCondition.LHS newLhs = bindGrpcValues(where.lhs());

    Term newValue = bindGrpcValues(where.value());

    if (newValue != where.value() || newLhs != where.lhs()) {
      where = BuiltCondition.of(newLhs, where.predicate(), newValue);
    }
    return where;
  }

  private static final String COUNT_FUNCTION_NAME = "COUNT";

  private String formatFunctionCall(FunctionCall functionCall) {
    StringBuilder builder = new StringBuilder();
    if (functionCall.getColumnName() == null
        && COUNT_FUNCTION_NAME.equals(functionCall.getFunctionName())) {
      builder.append(functionCall.getFunctionName()).append("(1)");
    } else {
      builder
          .append(functionCall.getFunctionName())
          .append('(')
          .append(cqlName(functionCall.getColumnName()));
      if (functionCall.getVectorTerm() != null) {
        builder.append(", ").append(formatValue(functionCall.getVectorTerm()));
      }
      builder.append(')');
    }
    if (functionCall.getAlias() != null) {
      builder.append(" AS ").append(cqlName(functionCall.getAlias()));
    }

    return builder.toString();
  }

  public static class FunctionCall {
    final String columnName;
    String alias;
    final String functionName;

    final Term vectorTerm;

    private FunctionCall(String columnName, String alias, String functionName, Term vectorTerm) {
      this.columnName = columnName;
      this.alias = alias;
      this.functionName = functionName;
      this.vectorTerm = vectorTerm;
    }

    public static FunctionCall function(String name, String alias, String functionName) {
      return new FunctionCall(name, alias, functionName, null);
    }

    public static FunctionCall function(
        String name, String alias, String functionName, Term vectorTerm) {
      return new FunctionCall(name, alias, functionName, vectorTerm);
    }

    public static FunctionCall count() {
      return count(null, null);
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

    public static FunctionCall similarityCosine(String columnName, Term vectorTerm) {
      return function(columnName, null, "SIMILARITY_COSINE", vectorTerm);
    }

    public static FunctionCall similarityCosine(String columnName, String alias, Term vectorTerm) {
      return function(columnName, alias, "SIMILARITY_COSINE", vectorTerm);
    }

    public static FunctionCall similarityDotProduct(String columnName, Term vectorTerm) {
      return function(columnName, null, "SIMILARITY_DOT_PRODUCT", vectorTerm);
    }

    public static FunctionCall similarityDotProduct(
        String columnName, String alias, Term vectorTerm) {
      return function(columnName, alias, "SIMILARITY_DOT_PRODUCT", vectorTerm);
    }

    public static FunctionCall similarityEuclidean(String columnName, Term vectorTerm) {
      return function(columnName, null, "SIMILARITY_EUCLIDEAN", vectorTerm);
    }

    public static FunctionCall similarityEuclidean(
        String columnName, String alias, Term vectorTerm) {
      return function(columnName, alias, "SIMILARITY_EUCLIDEAN", vectorTerm);
    }

    public void setAlias(String alias) {
      this.alias = alias;
    }

    public String getColumnName() {
      return columnName;
    }

    public String getFunctionName() {
      return functionName;
    }

    public String getAlias() {
      return alias;
    }

    public Term getVectorTerm() {
      return vectorTerm;
    }
  }
}
