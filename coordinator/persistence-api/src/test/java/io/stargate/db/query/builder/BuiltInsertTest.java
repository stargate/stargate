package io.stargate.db.query.builder;

import static io.stargate.db.query.BindMarker.markerFor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.db.query.BoundInsert;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.TypedValue;
import io.stargate.db.schema.Column.Type;
import org.junit.jupiter.api.Test;

class BuiltInsertTest extends BuiltDMLTest<BoundInsert> {
  BuiltInsertTest() {
    super(QueryType.INSERT);
  }

  @Test
  public void testInsertNoMarkers() {
    BuiltQuery<?> query =
        newBuilder()
            .insertInto(KS_NAME, "t1")
            .value("k1", "foo")
            .value("k2", 42)
            .value("v1", "foo_value")
            .value("v2", 1)
            .build();

    assertBuiltQuery(
        query,
        "INSERT INTO ks.t1 (k1, k2, v1, v2) VALUES ('foo', 42, 'foo_value', 1)",
        emptyList());

    setBound(query.bind());

    assertTestInsertBoundQuery("foo", 42L, "foo_value", 1);
  }

  @Test
  public void testInsertWithMarkers() {
    BuiltQuery<?> query = startTestInsertWithMarkers();
    setBound(query.bind("foo", "foo_value", 1));
    assertTestInsertBoundQuery("foo", 42L, "foo_value", 1);
  }

  @Test
  public void testInsertIfNotExists() {
    BuiltQuery<?> query =
        newBuilder()
            .insertInto(KS_NAME, "t1")
            .value("k1", "foo")
            .value("k2", 42)
            .value("v1", "foo_value")
            .value("v2", 1)
            .ifNotExists()
            .build();

    assertBuiltQuery(
        query,
        "INSERT INTO ks.t1 (k1, k2, v1, v2) VALUES ('foo', 42, 'foo_value', 1) IF NOT EXISTS",
        emptyList());

    setBound(query.bind());

    assertTestInsertBoundQuery(
        "INSERT INTO ks.t1 (k1, k2, v1, v2) VALUES (?, ?, ?, ?) IF NOT EXISTS",
        "foo",
        42L,
        "foo_value",
        1);
  }

  private BuiltQuery<?> startTestInsertWithMarkers() {
    BuiltQuery<?> query =
        newBuilder()
            .insertInto(KS_NAME, "t1")
            .value("k1")
            .value("k2", 42)
            .value("v1")
            .value("v2")
            .build();

    assertBuiltQuery(
        query,
        "INSERT INTO ks.t1 (k1, k2, v1, v2) VALUES (?, 42, ?, ?)",
        asList(markerFor("k1", Type.Text), markerFor("v1", Type.Text), markerFor("v2", Type.Int)));

    return query;
  }

  @Test
  public void testInsertWithNull() {
    BuiltQuery<?> query = startTestInsertWithMarkers();
    setBound(query.bind("foo", null, 1));
    assertTestInsertBoundQuery("foo", 42L, null, 1);
  }

  @Test
  public void testInsertWithUnset() {
    BuiltQuery<?> query = startTestInsertWithMarkers();

    setBound(query.bind("foo", TypedValue.UNSET, 1));

    assertBoundQuery(
        bound,
        "INSERT INTO ks.t1 (k1, k2, v1, v2) VALUES (?, ?, ?, ?)",
        // Note: because k2 is a bigint, we will get its value as a long, even though we input it as
        // a int. This is very much expected.
        asList("foo", 42L, TypedValue.UNSET, 1));

    assertThat(bound.table().name()).isEqualTo("t1");
    assertThat(keyToJava(bound.primaryKey())).isEqualTo(asList("foo", 42L));
    assertThat(bound.modifications()).isEqualTo(asList(set("v2", 1)));
    assertThat(bound.ttl()).isEmpty();
    assertThat(bound.timestamp()).isEmpty();
  }

  // Once we've bound values to the BuiltQuery, both the test with and without markers should yield
  // the same thing (for the same values), so this just avoid code duplication.
  private void assertTestInsertBoundQuery(String k1, long k2, String v1, int v2) {
    assertTestInsertBoundQuery(
        "INSERT INTO ks.t1 (k1, k2, v1, v2) VALUES (?, ?, ?, ?)", k1, k2, v1, v2);
  }

  private void assertTestInsertBoundQuery(
      String expectedBoundQuery, String k1, long k2, String v1, int v2) {
    assertBoundQuery(bound, expectedBoundQuery, asList(k1, k2, v1, v2));

    assertThat(bound.table().name()).isEqualTo("t1");
    assertThat(keyToJava(bound.primaryKey())).isEqualTo(asList(k1, k2));
    assertThat(bound.modifications()).isEqualTo(asList(set("v1", v1), set("v2", v2)));
    assertThat(bound.ttl()).isEmpty();
    assertThat(bound.timestamp()).isEmpty();
  }

  @Test
  public void testInsertUnknownColumnThrows() {
    assertThatThrownBy(
            () ->
                newBuilder()
                    .insertInto(KS_NAME, "t1")
                    .value("k1", "foo")
                    .value("random_name", 42)
                    .value("v1", "foo_value")
                    .value("v2", 1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find column random_name");
  }

  @Test
  public void testInsertIncompatibleValueThrows() {
    assertThatThrownBy(
            () ->
                newBuilder()
                    .insertInto(KS_NAME, "t1")
                    .value("k1", "foo")
                    .value("k2", 42)
                    .value("v1", 3) // v1 is text, so we won't be able to serialize this value
                    .value("v2", 1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Java value 3 of type 'java.lang.Integer' is not a valid value for CQL type text");
  }
}
