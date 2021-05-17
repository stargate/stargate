package io.stargate.db.query.builder;

import static io.stargate.db.query.BindMarker.markerFor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.QueryType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BuiltSelectTest extends BuiltQueryTest {
  BuiltSelectTest() {
    super(QueryType.SELECT);
  }

  private Set<String> names(Collection<Column> columns) {
    return columns.stream().map(Column::name).collect(Collectors.toSet());
  }

  private Set<String> asSet(String... values) {
    return new HashSet<>(asList(values));
  }

  @Test
  public void testSelectStarNoWhere() {
    QueryBuilder builder = newBuilder();

    BuiltQuery<?> query = builder.select().from(KS_NAME, "t1").build();

    assertBuiltQuery(query, "SELECT * FROM ks.t1", emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(select, "SELECT * FROM ks.t1");

    assertThat(select.isStarSelect()).isTrue();
    assertThat(select.selectedColumns()).isEmpty();
  }

  @Test
  public void testSelectColumnsNoWhere() {
    QueryBuilder builder = newBuilder();

    BuiltQuery<?> query = builder.select().column("k2", "v1").from(KS_NAME, "t1").build();

    assertBuiltQuery(query, "SELECT k2, v1 FROM ks.t1", emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(select, "SELECT k2, v1 FROM ks.t1");

    assertThat(select.isStarSelect()).isFalse();
    assertThat(names(select.selectedColumns())).isEqualTo(asSet("k2", "v1"));
  }

  @ParameterizedTest
  @MethodSource("functionsToTest")
  public void testFunctions(BuiltQuery<?> query, String expectedQueryString) {
    assertBuiltQuery(query, expectedQueryString, emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(select, expectedQueryString);

    assertThat(select.isStarSelect()).isFalse();
    assertThat(names(select.selectedColumns())).isEqualTo(asSet("k2", "v1", "v2"));
  }

  public static Stream<Arguments> functionsToTest() {
    Supplier<QueryBuilder.QueryBuilder__20> base = () -> newBuilder().select().column("k2", "v1");

    return Stream.of(
        arguments(
            base.get().writeTimeColumn("v2").from(KS_NAME, "t1").build(),
            "SELECT k2, v1, WRITETIME(v2) FROM ks.t1"),
        arguments(
            base.get().writeTimeColumn("v2").as("column_alias").from(KS_NAME, "t1").build(),
            "SELECT k2, v1, WRITETIME(v2) AS column_alias FROM ks.t1"),
        arguments(
            base.get().count("v2").from(KS_NAME, "t1").build(),
            "SELECT k2, v1, COUNT(v2) FROM ks.t1"),
        arguments(
            base.get().count("v2").as("column_alias").from(KS_NAME, "t1").build(),
            "SELECT k2, v1, COUNT(v2) AS column_alias FROM ks.t1"),
        arguments(
            base.get().max("v2").from(KS_NAME, "t1").build(), "SELECT k2, v1, MAX(v2) FROM ks.t1"),
        arguments(
            base.get().max("v2").as("column_alias").from(KS_NAME, "t1").build(),
            "SELECT k2, v1, MAX(v2) AS column_alias FROM ks.t1"),
        arguments(
            base.get().min("v2").from(KS_NAME, "t1").build(), "SELECT k2, v1, MIN(v2) FROM ks.t1"),
        arguments(
            base.get().min("v2").as("column_alias").from(KS_NAME, "t1").build(),
            "SELECT k2, v1, MIN(v2) AS column_alias FROM ks.t1"),
        arguments(
            base.get().sum("v2").from(KS_NAME, "t1").build(), "SELECT k2, v1, SUM(v2) FROM ks.t1"),
        arguments(
            base.get().sum("v2").as("column_alias").from(KS_NAME, "t1").build(),
            "SELECT k2, v1, SUM(v2) AS column_alias FROM ks.t1"),
        arguments(
            base.get().avg("v2").from(KS_NAME, "t1").build(), "SELECT k2, v1, AVG(v2) FROM ks.t1"),
        arguments(
            base.get().avg("v2").as("column_alias").from(KS_NAME, "t1").build(),
            "SELECT k2, v1, AVG(v2) AS column_alias FROM ks.t1"));
  }

  @Test
  public void shouldCreateOneQueryWithAllAggregations() {
    BuiltQuery<?> query =
        newBuilder()
            .select()
            .column("k2", "v1")
            .count("v1")
            .as("count")
            .max("v1")
            .as("max")
            .min("v1")
            .as("min")
            .sum("v1")
            .as("sum")
            .avg("v1")
            .as("avg")
            .from(KS_NAME, "t1")
            .build();

    assertBuiltQuery(
        query,
        "SELECT k2, v1, COUNT(v1) AS count, MAX(v1) AS max, MIN(v1) AS min, SUM(v1) AS sum, AVG(v1) AS avg FROM ks.t1",
        emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(
        select,
        "SELECT k2, v1, COUNT(v1) AS count, MAX(v1) AS max, MIN(v1) AS min, SUM(v1) AS sum, AVG(v1) AS avg FROM ks.t1");

    assertThat(select.isStarSelect()).isFalse();
    assertThat(names(select.selectedColumns())).isEqualTo(asSet("k2", "v1"));
  }

  @Test
  public void shouldPassAllFunctionCallsAsArgument() {
    // given
    List<QueryBuilderImpl.FunctionCall> functionCalls =
        Arrays.asList(
            QueryBuilderImpl.FunctionCall.avg("col1"),
            QueryBuilderImpl.FunctionCall.sum("col1"),
            QueryBuilderImpl.FunctionCall.max("col1"),
            QueryBuilderImpl.FunctionCall.min("col1"),
            QueryBuilderImpl.FunctionCall.writeTime("col1"),
            QueryBuilderImpl.FunctionCall.count("col1"));

    BuiltQuery<?> query =
        newBuilder()
            .select()
            .column("k2", "v1")
            .function(functionCalls)
            .from(KS_NAME, "t1")
            .build();
    assertBuiltQuery(
        query,
        "SELECT k2, v1, AVG(col1), SUM(col1), MAX(col1), MIN(col1), WRITETIME(col1), COUNT(col1) FROM ks.t1",
        emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(
        select,
        "SELECT k2, v1, AVG(col1), SUM(col1), MAX(col1), MIN(col1), WRITETIME(col1), COUNT(col1) FROM ks.t1");
  }

  @Test
  public void shouldPassAllFunctionCallsWithAliasAsArgument() {
    // given
    List<QueryBuilderImpl.FunctionCall> functionCalls =
        Arrays.asList(
            QueryBuilderImpl.FunctionCall.avg("col1", "avg"),
            QueryBuilderImpl.FunctionCall.sum("col1", "sum"),
            QueryBuilderImpl.FunctionCall.max("col1", "max"),
            QueryBuilderImpl.FunctionCall.min("col1", "min"),
            QueryBuilderImpl.FunctionCall.writeTime("col1", "writeTime"),
            QueryBuilderImpl.FunctionCall.count("col1", "count"));

    BuiltQuery<?> query =
        newBuilder()
            .select()
            .column("k2", "v1")
            .function(functionCalls)
            .from(KS_NAME, "t1")
            .build();
    assertBuiltQuery(
        query,
        "SELECT k2, v1, AVG(col1) AS avg, SUM(col1) AS sum, MAX(col1) AS max, MIN(col1) AS min, WRITETIME(col1) AS \"writeTime\", COUNT(col1) AS count FROM ks.t1",
        emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(
        select,
        "SELECT k2, v1, AVG(col1) AS avg, SUM(col1) AS sum, MAX(col1) AS max, MIN(col1) AS min, WRITETIME(col1) AS \"writeTime\", COUNT(col1) AS count FROM ks.t1");
  }

  @Test
  public void shouldCreateOneQueryWithAllAggregationsUsingColumnAPI() {
    Column column = Column.create("v1", Column.Kind.Regular);
    BuiltQuery<?> query =
        newBuilder()
            .select()
            .column("k2", "v1")
            .count(column)
            .as("count")
            .max(column)
            .as("max")
            .min(column)
            .as("min")
            .sum(column)
            .as("sum")
            .avg(column)
            .as("avg")
            .from(KS_NAME, "t1")
            .build();

    assertBuiltQuery(
        query,
        "SELECT k2, v1, COUNT(v1) AS count, MAX(v1) AS max, MIN(v1) AS min, SUM(v1) AS sum, AVG(v1) AS avg FROM ks.t1",
        emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(
        select,
        "SELECT k2, v1, COUNT(v1) AS count, MAX(v1) AS max, MIN(v1) AS min, SUM(v1) AS sum, AVG(v1) AS avg FROM ks.t1");

    assertThat(select.isStarSelect()).isFalse();
    assertThat(names(select.selectedColumns())).isEqualTo(asSet("k2", "v1"));
  }

  @Test
  public void shouldWorkForACaseSensitiveColumnName() {
    BuiltQuery<?> query =
        newBuilder()
            .select()
            .column("k2", "v1")
            .count("caseSensitiveCol")
            .from(KS_NAME, "t1")
            .build();

    assertBuiltQuery(query, "SELECT k2, v1, COUNT(\"caseSensitiveCol\") FROM ks.t1", emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(select, "SELECT k2, v1, COUNT(\"caseSensitiveCol\") FROM ks.t1");

    assertThat(select.isStarSelect()).isFalse();
    assertThat(names(select.selectedColumns())).isEqualTo(asSet("k2", "v1", "caseSensitiveCol"));
  }

  @Test
  public void shouldCreateOneQueryWithNSameAggregations() {
    BuiltQuery<?> query =
        newBuilder()
            .select()
            .column("k2", "v1")
            .count("v1")
            .as("count")
            .count("k2")
            .as("second_count")
            .from(KS_NAME, "t1")
            .build();

    assertBuiltQuery(
        query,
        "SELECT k2, v1, COUNT(v1) AS count, COUNT(k2) AS second_count FROM ks.t1",
        emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(
        select, "SELECT k2, v1, COUNT(v1) AS count, COUNT(k2) AS second_count FROM ks.t1");

    assertThat(select.isStarSelect()).isFalse();
    assertThat(names(select.selectedColumns())).isEqualTo(asSet("k2", "v1"));
  }

  @Test
  public void testSelectColumnsWithWhereNoMarkers() {
    QueryBuilder builder = newBuilder();

    BuiltQuery<?> query =
        builder
            .select()
            .column("k2", "v1")
            .from(KS_NAME, "t1")
            .where("k1", Predicate.EQ, "foo")
            .where("k2", Predicate.GTE, 3)
            .limit(42)
            .build();

    assertBuiltQuery(
        query, "SELECT k2, v1 FROM ks.t1 WHERE k1 = 'foo' AND k2 >= 3 LIMIT 42", emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(
        select, "SELECT k2, v1 FROM ks.t1 WHERE k1 = ? AND k2 >= ? LIMIT ?", "foo", 3L, 42);

    assertThat(select.isStarSelect()).isFalse();
    assertThat(names(select.selectedColumns())).isEqualTo(asSet("k2", "v1"));
  }

  @Test
  public void testSelectColumnsWithWhereAndMarkers() {
    QueryBuilder builder = newBuilder();

    BuiltQuery<?> query =
        builder
            .select()
            .column("k2", "v1")
            .from(KS_NAME, "t1")
            .where("k1", Predicate.EQ)
            .where("k2", Predicate.GTE)
            .limit()
            .build();

    assertBuiltQuery(
        query,
        "SELECT k2, v1 FROM ks.t1 WHERE k1 = ? AND k2 >= ? LIMIT ?",
        asList(
            markerFor("k1", Type.Text),
            markerFor("k2", Type.Bigint),
            markerFor("[limit]", Type.Int)));

    BoundSelect select = checkedCast(query.bind("foo", 3L, 42));

    assertBoundQuery(
        select, "SELECT k2, v1 FROM ks.t1 WHERE k1 = ? AND k2 >= ? LIMIT ?", "foo", 3L, 42);

    assertThat(select.isStarSelect()).isFalse();
    assertThat(names(select.selectedColumns())).isEqualTo(asSet("k2", "v1"));
  }

  @Test
  public void testSelectWhereIn() {
    QueryBuilder builder = newBuilder();

    BuiltQuery<?> query =
        builder
            .select()
            .from(KS_NAME, "t1")
            .where("k1", Predicate.IN, asList("foo", "bar"))
            .where("k2", Predicate.IN)
            .build();

    assertBuiltQuery(
        query,
        "SELECT * FROM ks.t1 WHERE k1 IN ('foo', 'bar') AND k2 IN ?",
        asList(markerFor("IN(k2)", Type.List.of(Type.Bigint))));

    // Note: forcing the call to bind(Object...)
    BoundSelect select = checkedCast(query.bind((Object) asList(3L, 2L, 1L)));

    assertBoundQuery(
        select,
        "SELECT * FROM ks.t1 WHERE k1 IN ? AND k2 IN ?",
        asList("foo", "bar"),
        asList(3L, 2L, 1L));

    assertThat(select.isStarSelect()).isTrue();
    assertThat(select.selectedColumns()).isEmpty();
  }

  @Test
  public void testSelectWithPerPartitionLimit() {
    assertBuiltQuery(
        newBuilder().select().star().from(KS_NAME, "t1").perPartitionLimit(123).build(),
        "SELECT * FROM ks.t1 PER PARTITION LIMIT 123",
        emptyList());

    assertBuiltQuery(
        newBuilder().select().star().from(KS_NAME, "t1").perPartitionLimit().build(),
        "SELECT * FROM ks.t1 PER PARTITION LIMIT ?",
        markerFor("[per-partition-limit]", Type.Int));

    assertBuiltQuery(
        newBuilder().select().star().from(KS_NAME, "t1").perPartitionLimit(123).limit(456).build(),
        "SELECT * FROM ks.t1 PER PARTITION LIMIT 123 LIMIT 456",
        emptyList());

    assertBuiltQuery(
        newBuilder().select().star().from(KS_NAME, "t1").perPartitionLimit().limit().build(),
        "SELECT * FROM ks.t1 PER PARTITION LIMIT ? LIMIT ?",
        markerFor("[per-partition-limit]", Type.Int),
        markerFor("[limit]", Type.Int));

    assertBuiltQuery(
        newBuilder().select().star().from(KS_NAME, "t1").perPartitionLimit(123).limit().build(),
        "SELECT * FROM ks.t1 PER PARTITION LIMIT 123 LIMIT ?",
        markerFor("[limit]", Type.Int));

    assertBuiltQuery(
        newBuilder().select().star().from(KS_NAME, "t1").perPartitionLimit().limit(456).build(),
        "SELECT * FROM ks.t1 PER PARTITION LIMIT ? LIMIT 456",
        markerFor("[per-partition-limit]", Type.Int));
  }
}
