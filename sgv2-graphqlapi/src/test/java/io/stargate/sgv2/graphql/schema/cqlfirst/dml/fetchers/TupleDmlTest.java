package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import com.jayway.jsonpath.JsonPath;
import graphql.ExecutionResult;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.Uuids;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TupleDmlTest extends DmlTestBase {

  @Override
  protected List<CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.TUPLES);
  }

  @ParameterizedTest
  @MethodSource("getValues")
  public void shouldParseTupleValues(int tupleIndex, Object[] values) {
    String columnName = String.format("value%d", tupleIndex);
    String graphql =
        String.format(
            "mutation { inserttuples(value: { %s:%s, id:1 }) { applied } }",
            columnName, toGraphQLValue(values));
    String expectedCQL =
        String.format("INSERT INTO tuples_ks.tuples (id, %1$s) VALUES (?, ?)", columnName);

    assertQuery(graphql, expectedCQL, ImmutableList.of(Values.of(1), toTupleValue(values)));
  }

  @ParameterizedTest
  @MethodSource("getValues")
  public void shouldEncodeTupleValues(int tupleIndex, Object[] values) {
    CqlTable table = SampleKeyspaces.TUPLES.getTables(0);
    ColumnSpec column = table.getColumns(tupleIndex);
    String tupleProps =
        IntStream.range(0, column.getType().getTuple().getElementsCount())
            .mapToObj(i -> "item" + i)
            .collect(Collectors.joining(","));
    String graphql =
        String.format("query { tuples { values { %s { %s } } } }", column.getName(), tupleProps);

    mockResultSet(
        singleRowResultSet(
            Collections.singletonList(column), Collections.singletonList(toTupleValue(values))));
    ExecutionResult result = executeGraphql(graphql);

    assertThat(result.getErrors()).isEmpty();
    Map<String, Object> data = result.getData();
    for (int i = 0; i < values.length; i++) {
      String jsonPath = String.format("tuples.values[0].%s.item%d", column.getName(), i);
      Object expected = toGraphql(values[i]);
      assertThat(JsonPath.<Object>read(data, jsonPath)).isEqualTo(expected);
    }
  }

  private Object toGraphql(Object value) {
    return (value instanceof Number || value instanceof Boolean || value == null)
        ? value
        : value.toString();
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Stream<Arguments> getValues() {
    return Stream.of(
        arguments(0, new Object[] {1.3f, -0.9f}),
        arguments(0, new Object[] {-1f, null}),
        arguments(0, new Object[] {null, 0f}),
        arguments(0, new Object[] {null, null}),
        arguments(0, new Object[] {Float.MAX_VALUE, 1f}),
        arguments(1, new Object[] {UUID.randomUUID(), "hello", 1}),
        arguments(1, new Object[] {UUID.randomUUID(), "second", null}),
        arguments(1, new Object[] {null, "third", null}),
        arguments(1, new Object[] {UUID.randomUUID(), null, null}),
        arguments(2, new Object[] {Uuids.timeBased(), true}));
  }

  private String toGraphQLValue(Object[] values) {
    StringBuilder builder = new StringBuilder("{ ");
    for (int i = 0; i < values.length; i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append("item");
      builder.append(i);
      builder.append(": ");
      Object v = values[i];
      if (v instanceof String || v instanceof UUID) {
        builder.append('"');
        builder.append(v);
        builder.append('"');
      } else {
        builder.append(v);
      }
    }
    builder.append(" }");
    return builder.toString();
  }

  private Value toTupleValue(Object[] objects) {
    Value[] values = new Value[objects.length];
    for (int i = 0; i < objects.length; i++) {
      Object o = objects[i];
      Value value;
      if (o == null) {
        value = Values.NULL;
      } else if (o instanceof Float) {
        value = Values.of((Float) o);
      } else if (o instanceof UUID) {
        value = Values.of((UUID) o);
      } else if (o instanceof String) {
        value = Values.of((String) o);
      } else if (o instanceof Boolean) {
        value = Values.of((Boolean) o);
      } else if (o instanceof Integer) {
        value = Values.of(((Integer) o));
      } else {
        throw new AssertionError("Unexpected type " + o.getClass().getName());
      }
      values[i] = value;
    }
    return Values.of(values);
  }
}
