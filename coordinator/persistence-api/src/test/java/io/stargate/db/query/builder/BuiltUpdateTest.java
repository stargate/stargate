package io.stargate.db.query.builder;

import static io.stargate.db.query.BindMarker.markerFor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.db.query.BoundUpdate;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.TypedValue;
import io.stargate.db.schema.Column.Type;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class BuiltUpdateTest extends BuiltDMLTest<BoundUpdate> {
  BuiltUpdateTest() {
    super(QueryType.UPDATE);
  }

  @Test
  public void testUpdateNoMarkers() {
    BuiltQuery<?> query =
        newBuilder()
            .update(KS_NAME, "t1")
            .ttl(42)
            .timestamp(1L)
            .value("v1", "foo_value")
            .value("v2", 1)
            .where("k1", Predicate.EQ, "foo")
            .where("k2", Predicate.EQ, 42)
            .build();

    assertBuiltQuery(
        query,
        "UPDATE ks.t1 USING TTL 42 AND TIMESTAMP 1 SET v1 = 'foo_value', v2 = 1 WHERE k1 = 'foo' AND k2 = 42",
        emptyList());

    setBound(query.bind());

    assertTestUpdateBoundQuery(42, 1L, "foo", 42L, "foo_value", 1);
  }

  @Test
  public void testUpdateIfExists() {
    BuiltQuery<?> query =
        newBuilder()
            .update(KS_NAME, "t1")
            .value("v1", "foo_value")
            .value("v2", 1)
            .where("k1", Predicate.EQ, "foo")
            .where("k2", Predicate.EQ, 42)
            .ifExists()
            .build();

    assertBuiltQuery(
        query,
        "UPDATE ks.t1 SET v1 = 'foo_value', v2 = 1 WHERE k1 = 'foo' AND k2 = 42 IF EXISTS",
        emptyList());

    setBound(query.bind());

    assertTestUpdateBoundQuery(
        "UPDATE ks.t1 SET v1 = ?, v2 = ? WHERE k1 = ? AND k2 = ? IF EXISTS",
        null,
        null,
        "foo",
        42L,
        "foo_value",
        1);
  }

  @Test
  public void testUpdateWithMarkers() {
    BuiltQuery<?> query = startTestUpdateWithMarkers();
    setBound(query.bind(42, 1L, "foo_value", 1, "foo"));
    assertTestUpdateBoundQuery(42, 1L, "foo", 42L, "foo_value", 1);
  }

  private BuiltQuery<?> startTestUpdateWithMarkers() {
    BuiltQuery<?> query =
        newBuilder()
            .update(KS_NAME, "t1")
            .ttl()
            .timestamp()
            .value("v1")
            .value("v2")
            .where("k1", Predicate.EQ)
            .where("k2", Predicate.EQ, 42)
            .build();

    assertBuiltQuery(
        query,
        "UPDATE ks.t1 USING TTL ? AND TIMESTAMP ? SET v1 = ?, v2 = ? WHERE k1 = ? AND k2 = 42",
        asList(
            markerFor("[ttl]", Type.Int),
            markerFor("[timestamp]", Type.Bigint),
            markerFor("v1", Type.Text),
            markerFor("v2", Type.Int),
            markerFor("k1", Type.Text)));

    return query;
  }

  @Test
  public void testInsertWithNull() {
    BuiltQuery<?> query = startTestUpdateWithMarkers();
    setBound(query.bind(42, 1L, null, 1, "foo"));
    assertTestUpdateBoundQuery(42, 1L, "foo", 42L, null, 1);
  }

  @Test
  public void testInsertWithUnset() {
    BuiltQuery<?> query = startTestUpdateWithMarkers();

    setBound(query.bind(TypedValue.UNSET, 2L, TypedValue.UNSET, 1, "foo"));

    assertBoundQuery(
        bound,
        "UPDATE ks.t1 USING TTL ? AND TIMESTAMP ? SET v1 = ?, v2 = ? WHERE k1 = ? AND k2 = ?",
        // Note: because k2 is a bigint, we will get its value as a long, even though we input it as
        // a int. This is very much expected.
        asList(TypedValue.UNSET, 2L, TypedValue.UNSET, 1, "foo", 42L));

    assertThat(bound.table().name()).isEqualTo("t1");
    assertThat(keysToJava(bound.primaryKeys())).isEqualTo(asList(asList("foo", 42L)));
    assertThat(bound.modifications()).isEqualTo(asList(set("v2", 1)));
    assertThat(bound.ttl()).isEmpty();
    assertThat(bound.timestamp()).hasValue(2L);
  }

  // Once we've bound values to the BuiltQuery, both the test with and without markers should yield
  // the same thing (for the same values), so this just avoid code duplication.
  private void assertTestUpdateBoundQuery(
      int ttl, long timestamp, String k1, long k2, String v1, int v2) {
    assertTestUpdateBoundQuery(
        "UPDATE ks.t1 USING TTL ? AND TIMESTAMP ? SET v1 = ?, v2 = ? WHERE k1 = ? AND k2 = ?",
        ttl,
        timestamp,
        k1,
        k2,
        v1,
        v2);
  }

  private void assertTestUpdateBoundQuery(
      String expectedBoundQuery,
      Integer ttl,
      Long timestamp,
      String k1,
      long k2,
      String v1,
      int v2) {
    List<Object> expectedBoundValues = new ArrayList<>();
    if (ttl != null) expectedBoundValues.add(ttl);
    if (timestamp != null) expectedBoundValues.add(timestamp);
    expectedBoundValues.addAll(asList(v1, v2, k1, k2));
    assertBoundQuery(bound, expectedBoundQuery, expectedBoundValues);

    assertThat(bound.table().name()).isEqualTo("t1");
    assertThat(keysToJava(bound.primaryKeys())).isEqualTo(asList(asList(k1, k2)));
    assertThat(bound.modifications()).isEqualTo(asList(set("v1", v1), set("v2", v2)));
    if (ttl == null) {
      assertThat(bound.ttl()).isEmpty();
    } else {
      assertThat(bound.ttl()).hasValue(ttl);
    }
    if (timestamp == null) {
      assertThat(bound.timestamp()).isEmpty();
    } else {
      assertThat(bound.timestamp()).hasValue(timestamp);
    }
  }

  @Test
  public void testUpdateUnknownColumnThrows() {
    assertThatThrownBy(
            () ->
                newBuilder()
                    .update(KS_NAME, "t1")
                    .value("v1", "foo_value")
                    .value("v2", 1)
                    .where("random_name", Predicate.EQ, "foo")
                    .where("k2", Predicate.EQ, 42)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find column random_name");
  }

  @Test
  public void testUpdateIncompatibleValueThrows() {
    assertThatThrownBy(
            () ->
                newBuilder()
                    .update(KS_NAME, "t1")
                    .value("v1", "foo_value")
                    .value("v2", 1)
                    .where("k1", Predicate.EQ, 3) // k1 is text so ...
                    .where("k2", Predicate.EQ, 42)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Java value 3 of type 'java.lang.Integer' is not a valid value for CQL type text");
  }
}
