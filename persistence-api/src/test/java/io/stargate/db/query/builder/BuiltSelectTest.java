package io.stargate.db.query.builder;

import static io.stargate.db.query.BindMarker.markerFor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.QueryType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

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
            .limit(42L)
            .build();

    assertBuiltQuery(
        query, "SELECT k2, v1 FROM ks.t1 WHERE k1 = 'foo' AND k2 >= 3 LIMIT 42", emptyList());

    BoundSelect select = checkedCast(query.bind());

    assertBoundQuery(
        select, "SELECT k2, v1 FROM ks.t1 WHERE k1 = ? AND k2 >= ? LIMIT ?", "foo", 3L, 42L);

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
            markerFor("[limit]", Type.Bigint)));

    BoundSelect select = checkedCast(query.bind("foo", 3L, 42L));

    assertBoundQuery(
        select, "SELECT k2, v1 FROM ks.t1 WHERE k1 = ? AND k2 >= ? LIMIT ?", "foo", 3L, 42L);

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
}
