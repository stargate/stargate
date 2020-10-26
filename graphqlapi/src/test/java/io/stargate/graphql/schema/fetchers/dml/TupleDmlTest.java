package io.stargate.graphql.schema.fetchers.dml;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.DmlTestBase;
import java.util.UUID;
import java.util.stream.Stream;
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
            getColumn("value1", Type.Uuid, Type.Text, Type.Int))
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
  public Keyspace getKeyspace() {
    return keyspace;
  }

  @ParameterizedTest
  @MethodSource("getValues")
  public void shouldParseTupleValues(int tupleIndex, Object[] values) {
    String column = String.format("value%d", tupleIndex);
    String mutation = "mutation { insertTuples(value: { %s:%s, id:1 }) { applied } }";
    String expectedCQL =
        String.format(
            "INSERT INTO tuples_ks.tuples (id,%s) VALUES (1,%s)", column, toCqlLiteral(values));

    assertSuccess(String.format(mutation, column, toGraphQLValue(values)), expectedCQL);
  }

  private static Stream<Arguments> getValues() {
    return Stream.of(
        arguments(0, new Object[] {1.3f, -0.9f}),
        arguments(0, new Object[] {-1f}),
        arguments(0, new Object[] {Float.MAX_VALUE, 1f}),
        arguments(1, new Object[] {UUID.randomUUID(), "hello", 1}),
        arguments(1, new Object[] {UUID.randomUUID(), "second"}));
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
      } else {
        builder.append(v);
      }
    }
    builder.append(")");
    return builder.toString();
  }
}
