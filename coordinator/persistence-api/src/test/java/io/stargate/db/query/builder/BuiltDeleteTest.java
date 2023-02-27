package io.stargate.db.query.builder;

import static io.stargate.db.query.BindMarker.markerFor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.db.query.BoundDelete;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.PrimaryKey;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.RowsRange;
import io.stargate.db.query.TypedValue;
import io.stargate.db.schema.Column.Type;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class BuiltDeleteTest extends BuiltDMLTest<BoundDelete> {

  BuiltDeleteTest() {
    super(QueryType.DELETE);
  }

  @Test
  public void testDeleteColumnNoMarkers() {
    QueryBuilder builder = newBuilder();

    BuiltQuery<?> query =
        builder
            .delete()
            .column("v2")
            .from(KS_NAME, "t1")
            .where("k1", Predicate.EQ, "foo")
            .where("k2", Predicate.EQ, 42)
            .build();

    assertBuiltQuery(query, "DELETE v2 FROM ks.t1 WHERE k1 = 'foo' AND k2 = 42", emptyList());

    setBound(query.bind());

    assertTestDeleteBoundQuery("foo", 42L);
  }

  @Test
  public void testDeleteIfExists() {
    QueryBuilder builder = newBuilder();

    BuiltQuery<?> query =
        builder
            .delete()
            .column("v2")
            .from(KS_NAME, "t1")
            .where("k1", Predicate.EQ, "foo")
            .where("k2", Predicate.EQ, 42)
            .ifExists()
            .build();

    assertBuiltQuery(
        query, "DELETE v2 FROM ks.t1 WHERE k1 = 'foo' AND k2 = 42 IF EXISTS", emptyList());

    setBound(query.bind());

    assertTestDeleteBoundQuery(
        "DELETE v2 FROM ks.t1 WHERE k1 = ? AND k2 = ? IF EXISTS", "foo", 42L);
  }

  @Test
  public void testDeleteWithMarkers() {
    BuiltQuery<?> query = startTestDeleteWithMarkers();
    setBound(query.bind("foo", 42));
    assertTestDeleteBoundQuery("foo", 42L);
  }

  private BuiltQuery<?> startTestDeleteWithMarkers() {
    QueryBuilder builder = newBuilder();

    BuiltQuery<?> query =
        builder
            .delete()
            .column("v2")
            .from(KS_NAME, "t1")
            .where("k1", Predicate.EQ)
            .where("k2", Predicate.EQ)
            .build();

    assertBuiltQuery(
        query,
        "DELETE v2 FROM ks.t1 WHERE k1 = ? AND k2 = ?",
        asList(markerFor("k1", Type.Text), markerFor("k2", Type.Bigint)));

    return query;
  }

  @Test
  public void testDeleteWithUnset() {
    BuiltQuery<?> query = startTestDeleteWithMarkers();

    setBound(query.bind("foo", TypedValue.UNSET));

    assertBoundQuery(
        bound, "DELETE v2 FROM ks.t1 WHERE k1 = ? AND k2 = ?", asList("foo", TypedValue.UNSET));

    assertThat(bound.table().name()).isEqualTo("t1");
    assertThat(bound.rowsUpdated().isRanges()).isTrue();
    List<RowsRange> ranges = bound.rowsUpdated().asRanges().ranges();
    assertThat(ranges.size()).isEqualTo(1);
    RowsRange range = ranges.get(0);
    assertThat(keyToJava(range.partitionKey())).isEqualTo(asList("foo"));
    assertThat(toJava(range.clusteringStart().values())).isEmpty();
    assertThat(toJava(range.clusteringEnd().values())).isEmpty();
    assertThat(bound.modifications()).isEqualTo(asList(set("v2", null)));
    assertThat(bound.ttl()).isEmpty();
    assertThat(bound.timestamp()).isEmpty();
  }

  @Test
  public void testDeleteWithIN() {
    QueryBuilder builder = newBuilder();

    BuiltQuery<?> query =
        builder
            .delete()
            .column("v2")
            .from(KS_NAME, "t1")
            .where("k1", Predicate.EQ, "foo")
            .where("k2", Predicate.IN, asList(1, 2, 3))
            .build();

    assertBuiltQuery(
        query, "DELETE v2 FROM ks.t1 WHERE k1 = 'foo' AND k2 IN (1, 2, 3)", emptyList());

    setBound(query.bind());

    assertBoundQuery(
        bound, "DELETE v2 FROM ks.t1 WHERE k1 = ? AND k2 IN ?", asList("foo", asList(1L, 2L, 3L)));

    assertThat(bound.table().name()).isEqualTo("t1");
    assertThat(bound.rowsUpdated().isKeys()).isTrue();
    List<PrimaryKey> keys = bound.rowsUpdated().asKeys().primaryKeys();
    assertThat(keysToJava(keys))
        .isEqualTo(asList(asList("foo", 1L), asList("foo", 2L), asList("foo", 3L)));
    assertThat(bound.modifications()).isEqualTo(asList(set("v2", null)));
    assertThat(bound.ttl()).isEmpty();
    assertThat(bound.timestamp()).isEmpty();
  }

  // Once we've bound values to the BuiltQuery, both the test with and without markers should yield
  // the same thing (for the same values), so this just avoid code duplication.
  private void assertTestDeleteBoundQuery(String k1, long k2) {
    assertTestDeleteBoundQuery("DELETE v2 FROM ks.t1 WHERE k1 = ? AND k2 = ?", k1, k2);
  }

  private void assertTestDeleteBoundQuery(String expectedBoundQuery, String k1, long k2) {
    assertBoundQuery(bound, expectedBoundQuery, asList(k1, k2));

    assertThat(bound.table().name()).isEqualTo("t1");
    assertThat(bound.rowsUpdated().isKeys()).isTrue();
    assertThat(keysToJava(bound.rowsUpdated().asKeys().primaryKeys()))
        .isEqualTo(asList(asList(k1, k2)));
    assertThat(bound.modifications()).isEqualTo(asList(set("v2", null)));
    assertThat(bound.ttl()).isEmpty();
    assertThat(bound.timestamp()).isEmpty();
  }

  @Test
  public void testDeleteUnknownColumnThrows() {
    QueryBuilder builder = newBuilder();
    assertThatThrownBy(
            () ->
                builder
                    .delete()
                    .column("v1")
                    .column("random_name")
                    .from(KS_NAME, "t1")
                    .where("k1", Predicate.EQ, "foo")
                    .where("k2", Predicate.EQ, 42)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find column random_name");
  }

  @Test
  public void testDeleteIncompatibleValueThrows() {
    QueryBuilder builder = newBuilder();
    assertThatThrownBy(
            () ->
                builder
                    .delete()
                    .column("v2")
                    .from(KS_NAME, "t1")
                    .where("k1", Predicate.EQ, 3) // k1 is text so ...
                    .where("k2", Predicate.EQ, 42)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Java value 3 of type 'java.lang.Integer' is not a valid value for CQL type text");
  }

  @Test
  public void testEQPredicateCondition() {
    // given, when
    BuiltQuery<?> query =
        queryWithCondition(BuiltCondition.of(BuiltCondition.LHS.column("v2"), Predicate.EQ, 100));

    // when
    assertBuiltQuery(query, "DELETE FROM ks.t1 WHERE k1 = 'foo' IF v2 = 100", emptyList());

    // when
    setBound(query.bind());

    // then
    assertTestDeleteBoundQueryRange("DELETE FROM ks.t1 WHERE k1 = ? IF v2 = ?", "foo", 100);
  }

  @Test
  public void testNEQPredicateCondition() {
    // given, when
    BuiltQuery<?> query =
        queryWithCondition(BuiltCondition.of(BuiltCondition.LHS.column("v2"), Predicate.NEQ, 100));

    // when
    assertBuiltQuery(query, "DELETE FROM ks.t1 WHERE k1 = 'foo' IF v2 != 100", emptyList());

    // when
    setBound(query.bind());

    // then
    assertTestDeleteBoundQueryRange("DELETE FROM ks.t1 WHERE k1 = ? IF v2 != ?", "foo", 100);
  }

  @Test
  public void testLTPredicateCondition() {
    // given, when
    BuiltQuery<?> query =
        queryWithCondition(BuiltCondition.of(BuiltCondition.LHS.column("v2"), Predicate.LT, 100));

    // when
    assertBuiltQuery(query, "DELETE FROM ks.t1 WHERE k1 = 'foo' IF v2 < 100", emptyList());

    // when
    setBound(query.bind());

    // then
    assertTestDeleteBoundQueryRange("DELETE FROM ks.t1 WHERE k1 = ? IF v2 < ?", "foo", 100);
  }

  @Test
  public void testGTPredicateCondition() {
    // given, when
    BuiltQuery<?> query =
        queryWithCondition(BuiltCondition.of(BuiltCondition.LHS.column("v2"), Predicate.GT, 100));

    // when
    assertBuiltQuery(query, "DELETE FROM ks.t1 WHERE k1 = 'foo' IF v2 > 100", emptyList());

    // when
    setBound(query.bind());

    // then
    assertTestDeleteBoundQueryRange("DELETE FROM ks.t1 WHERE k1 = ? IF v2 > ?", "foo", 100);
  }

  @Test
  public void testLTEPredicateCondition() {
    // given, when
    BuiltQuery<?> query =
        queryWithCondition(BuiltCondition.of(BuiltCondition.LHS.column("v2"), Predicate.LTE, 100));

    // when
    assertBuiltQuery(query, "DELETE FROM ks.t1 WHERE k1 = 'foo' IF v2 <= 100", emptyList());

    // when
    setBound(query.bind());

    // then
    assertTestDeleteBoundQueryRange("DELETE FROM ks.t1 WHERE k1 = ? IF v2 <= ?", "foo", 100);
  }

  @Test
  public void testGTEPredicateCondition() {
    // given, when
    BuiltQuery<?> query =
        queryWithCondition(BuiltCondition.of(BuiltCondition.LHS.column("v2"), Predicate.GTE, 100));

    // when
    assertBuiltQuery(query, "DELETE FROM ks.t1 WHERE k1 = 'foo' IF v2 >= 100", emptyList());

    // when
    setBound(query.bind());

    // then
    assertTestDeleteBoundQueryRange("DELETE FROM ks.t1 WHERE k1 = ? IF v2 >= ?", "foo", 100);
  }

  @Test
  public void testINPredicateCondition() {
    // given, when
    BuiltQuery<?> query =
        queryWithCondition(
            BuiltCondition.of(BuiltCondition.LHS.column("v2"), Predicate.IN, Arrays.asList(1, 2)));

    // when
    assertBuiltQuery(query, "DELETE FROM ks.t1 WHERE k1 = 'foo' IF v2 IN (1, 2)", emptyList());

    // when
    setBound(query.bind());

    // then
    assertTestDeleteBoundQueryRange(
        "DELETE FROM ks.t1 WHERE k1 = ? IF v2 IN ?", "foo", Arrays.asList(1, 2));
  }

  @Test
  public void testCONTAINSPredicateCondition() {
    // given, when
    BuiltQuery<?> query =
        queryWithCondition(
            BuiltCondition.of(BuiltCondition.LHS.column("list"), Predicate.CONTAINS, "a"));

    // when
    assertBuiltQuery(query, "DELETE FROM ks.t1 WHERE k1 = 'foo' IF list CONTAINS 'a'", emptyList());

    // when
    setBound(query.bind());

    // then
    assertTestDeleteBoundQueryRange(
        "DELETE FROM ks.t1 WHERE k1 = ? IF list CONTAINS ?", "foo", "a");
  }

  @Test
  public void testCONTAINS_KEYPredicateCondition() {
    // given, when
    BuiltQuery<?> query =
        queryWithCondition(
            BuiltCondition.of(BuiltCondition.LHS.column("map"), Predicate.CONTAINS_KEY, "a"));

    // when
    assertBuiltQuery(
        query, "DELETE FROM ks.t1 WHERE k1 = 'foo' IF map CONTAINS KEY 'a'", emptyList());

    // when
    setBound(query.bind());

    // then
    assertTestDeleteBoundQueryRange(
        "DELETE FROM ks.t1 WHERE k1 = ? IF map CONTAINS KEY ?", "foo", "a");
  }

  private BuiltQuery<?> queryWithCondition(BuiltCondition condition) {
    QueryBuilder builder = newBuilder();
    return builder
        .delete()
        .from(KS_NAME, "t1")
        .where("k1", Predicate.EQ, "foo")
        .ifs(condition)
        .ifExists(false)
        .build();
  }

  private void assertTestDeleteBoundQueryRange(String expectedBoundQuery, String k1, Object v) {
    assertBoundQuery(bound, expectedBoundQuery, asList(k1, v));

    assertThat(bound.table().name()).isEqualTo("t1");
    assertThat(bound.rowsUpdated().isRanges()).isTrue();
    assertThat(bound.ttl()).isEmpty();
    assertThat(bound.timestamp()).isEmpty();
  }
}
