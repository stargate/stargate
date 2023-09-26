package io.stargate.db.query.builder;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.PrimaryKey;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.SchemaKey;
import io.stargate.db.query.TypedValue;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.Schema;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BuiltQueryTest {

  protected static final String KS_NAME = "ks";
  protected static final Schema schema =
      Schema.build()
          .keyspace(KS_NAME)
          .table("t1")
          .column("k1", Type.Text, Kind.PartitionKey)
          .column("k2", Type.Bigint, Kind.Clustering)
          .column("s", Type.Text, Kind.Static)
          .column("v1", Type.Text, Kind.Regular)
          .column("v2", Type.Int, Kind.Regular)
          .column("caseSensitiveCol", Type.Int, Kind.Regular)
          .column("list", Type.List.of(Type.Text), Kind.Regular)
          .column("map", Type.Map.of(Type.Text, Type.Text), Kind.Regular)
          .build();

  private static final TypedValue.Codec valueCodec = TypedValue.Codec.testCodec();

  private final QueryType expectedQueryType;

  protected BuiltQueryTest(QueryType expectedQueryType) {
    this.expectedQueryType = expectedQueryType;
  }

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  protected <B extends BoundQuery> B checkedCast(BoundQuery bound) {
    try {
      return (B) bound;
    } catch (ClassCastException e) {
      return fail("Bound query is not of the expected class: %s", e.getMessage());
    }
  }

  protected TypedValue.Codec codec() {
    return valueCodec;
  }

  protected static QueryBuilderImpl newBuilder() {
    return new QueryBuilderImpl(schema, valueCodec, null);
  }

  protected List<Object> toJava(List<TypedValue> values) {
    return values.stream().map(TypedValue::javaValue).collect(Collectors.toList());
  }

  protected List<List<Object>> keysToJava(List<PrimaryKey> values) {
    return values.stream().map(this::keyToJava).collect(Collectors.toList());
  }

  protected List<Object> keyToJava(SchemaKey pk) {
    return toJava(pk.allValues());
  }

  protected void assertBuiltQuery(
      BuiltQuery<?> query, String expectedBuiltQueryString, BindMarker... expectedBindMarkers) {
    assertBuiltQuery(query, expectedBuiltQueryString, asList(expectedBindMarkers));
  }

  protected void assertBuiltQuery(
      BuiltQuery<?> query, String expectedBuiltQueryString, List<BindMarker> expectedBindMarkers) {
    assertThat(query.type()).isEqualTo(expectedQueryType);
    assertThat(query.toString()).isEqualTo(expectedBuiltQueryString);
    assertThat(query.bindMarkers()).isEqualTo(expectedBindMarkers);
  }

  protected void assertBoundQuery(
      BoundQuery bound, String expectedBoundQueryString, Object... expectedBoundValues) {
    assertBoundQuery(bound, expectedBoundQueryString, asList(expectedBoundValues));
  }

  protected void assertBoundQuery(
      BoundQuery bound, String expectedBoundQueryString, List<Object> expectedBoundValues) {
    assertThat(bound.type()).isEqualTo(expectedQueryType);
    assertThat(bound.queryString()).isEqualTo(expectedBoundQueryString);
    assertThat(toJava(bound.values())).isEqualTo(expectedBoundValues);
  }
}
