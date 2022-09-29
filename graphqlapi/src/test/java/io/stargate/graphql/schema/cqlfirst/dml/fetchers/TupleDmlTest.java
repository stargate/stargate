package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import graphql.ExecutionResult;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TupleDmlTest extends DmlTestBase {
  public static final Table table = buildTable();
  public static final Keyspace keyspace =
      ImmutableKeyspace.builder().name("tuples_ks").addTables(table).build();

  private static Table buildTable() {
    return ImmutableTable.builder()
        .keyspace("tuples_ks")
        .name("tuples")
        .addColumns(
            ImmutableColumn.builder()
                .keyspace("tuples")
                .table("tuples_table")
                .name("id")
                .type(Type.Int)
                .kind(Column.Kind.PartitionKey)
                .build(),
            getColumn("value0", Type.Float, Type.Float),
            getColumn("value1", Type.Uuid, Type.Text, Type.Int),
            getColumn("value2", Type.Timeuuid, Type.Boolean))
        .build();
  }

  private static ImmutableColumn getColumn(String name, ColumnType... types) {
    return ImmutableColumn.builder()
        .keyspace("tuples_ks")
        .table("tuples")
        .name(name)
        .type(Type.Tuple.of(types))
        .kind(Column.Kind.Regular)
        .build();
  }

  @Override
  public Schema getCQLSchema() {
    return Schema.create(Collections.singleton(keyspace));
  }

  @ParameterizedTest
  @MethodSource("getValues")
  public void shouldParseTupleValues(int tupleIndex, Object[] values) {
    String column = String.format("value%d", tupleIndex);
    String mutation = "mutation { inserttuples(value: { %s:%s, id:1 }) { applied } }";
    String expectedCQL =
        String.format(
            "INSERT INTO tuples_ks.tuples (id, %s) VALUES (1, %s)", column, toCqlLiteral(values));

    assertQuery(String.format(mutation, column, toGraphQLValue(values)), expectedCQL);
  }

  @ParameterizedTest
  @MethodSource("getValues")
  public void shouldEncodeTupleValues(int tupleIndex, Object[] values) {
    Column column = table.column(String.format("value%d", tupleIndex));
    String query = "query { tuples { values { %s { %s } } } }";
    String tupleProps =
        IntStream.range(0, column.type().parameters().size())
            .mapToObj(i -> "item" + i)
            .collect(Collectors.joining(","));
    Row row = createRowForSingleValue(column, values);
    when(resultSet.currentPageRows()).thenReturn(Collections.singletonList(row));
    ExecutionResult result = executeGraphQl(String.format(query, column.name(), tupleProps));
    assertThat(result.getErrors()).isEmpty();
    assertThat(result.<Map<String, Object>>getData())
        .extractingByKey("tuples", InstanceOfAssertFactories.MAP)
        .extractingByKey("values", InstanceOfAssertFactories.LIST)
        .singleElement()
        .asInstanceOf(InstanceOfAssertFactories.MAP)
        .extractingByKey(column.name())
        // Tuples are returned as an GraphQL Object { item0: v0, item1: v1, ... }
        // .asInstanceOf(InstanceOfAssertFactories.MAP)
        .isEqualTo(toMap(values, column));
  }

  private Row createRowForSingleValue(Column column, Object[] tupleValues) {
    Map<String, Object> values = new HashMap<>();
    values.put(column.name(), column.type().create(tupleValues));
    return createRow(Collections.singletonList(column), values);
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

  private String toCqlLiteral(Object[] values) {
    StringBuilder builder = new StringBuilder("(");
    for (int i = 0; i < values.length; i++) {
      if (i > 0) {
        builder.append(",");
      }
      Object v = values[i];
      if (v instanceof String) {
        builder.append("'");
        builder.append(v);
        builder.append("'");
      } else if (v == null) {
        builder.append("NULL");
      } else {
        builder.append(v);
      }
    }
    builder.append(")");
    return builder.toString();
  }

  private Map<String, Object> toMap(Object[] values, Column column) {
    Map<String, Object> result = new HashMap<>();
    for (int i = 0; i < column.type().parameters().size(); i++) {
      Object value = null;
      if (values.length > i) {
        value = values[i];

        if (value != null && !(value instanceof Number) && !(value instanceof Boolean)) {
          value = value.toString();
        }
      }
      result.put("item" + i, value);
    }
    return result;
  }
}
