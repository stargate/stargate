package io.stargate.grpc.service;

import static io.stargate.grpc.CqlHelper.newStatement;
import static io.stargate.grpc.CqlHelper.selectColumn;
import static io.stargate.grpc.CqlHelper.termOf;
import static io.stargate.grpc.CqlHelper.whereColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.list;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static stargate.Cql.Relation.Operator.EQ;

import io.stargate.grpc.CqlHelper;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import stargate.Cql;
import stargate.Cql.Select;

public class StatementStringifierTest {

  @ParameterizedTest
  @MethodSource("sampleStatements")
  public void shouldStringifyStatement(
      Cql.Statement statement,
      boolean alwaysInlineValues,
      String expectedCql,
      List<QueryOuterClass.Value> expectedValues) {
    StatementStringifier stringifier = new StatementStringifier(alwaysInlineValues);
    stringifier.append(statement);
    assertThat(stringifier.getCql()).isEqualTo(expectedCql);
    assertThat(stringifier.getValues()).isEqualTo(expectedValues);
  }

  public static Arguments[] sampleStatements() {
    return new Arguments[] {
      arguments(
          newStatement(Select.newBuilder().setKeyspaceName("ks").setTableName("tbl")),
          false,
          "SELECT * FROM \"ks\".\"tbl\"",
          Collections.emptyList()),
      arguments(
          newStatement(
              Select.newBuilder()
                  .setKeyspaceName("ks")
                  .setTableName("tbl")
                  .addSelectors(selectColumn("c1"))
                  .addSelectors(selectColumn("c2"))),
          false,
          "SELECT \"c1\",\"c2\" FROM \"ks\".\"tbl\"",
          Collections.emptyList()),
      arguments(
          newStatement(
              Select.newBuilder()
                  .setKeyspaceName("ks")
                  .setTableName("tbl")
                  .addSelectors(selectColumn("c1"))
                  .addSelectors(selectColumn("c2"))
                  .addWhere(whereColumn("k").setOperator(EQ).setTerm(termOf(1)))),
          true,
          "SELECT \"c1\",\"c2\" FROM \"ks\".\"tbl\" WHERE \"k\" = 1",
          Collections.emptyList()),
      arguments(
          newStatement(
              Select.newBuilder()
                  .setKeyspaceName("ks")
                  .setTableName("tbl")
                  .addSelectors(selectColumn("c1"))
                  .addSelectors(selectColumn("c2"))
                  .addWhere(whereColumn("k").setOperator(EQ).setTerm(termOf(1)))),
          false,
          "SELECT \"c1\",\"c2\" FROM \"ks\".\"tbl\" WHERE \"k\" = ?",
          list(Values.of(1))),
      arguments(
          newStatement(
              Select.newBuilder()
                  .setKeyspaceName("ks")
                  .setTableName("tbl")
                  .addSelectors(CqlHelper.selectToken("k1", "k2"))
                  .addWhere(whereColumn("k1").setOperator(EQ).setTerm(termOf(1)))
                  .addWhere(whereColumn("k2").setOperator(EQ).setTerm(termOf(1)))),
          false,
          "SELECT \"token\"(\"k1\",\"k2\") FROM \"ks\".\"tbl\" WHERE \"k1\" = ? AND \"k2\" = ?",
          list(Values.of(1), Values.of(1))),
    };
  }
}
