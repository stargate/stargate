package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryFetcherUdtsTest extends DmlTestBase {

  @Override
  protected List<CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.UDTS);
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL with UDTs and generate expected CQL query")
  public void udtTest(String graphQlQuery, String expectedCqlQuery, List<Value> expectedValues) {
    assertQuery(String.format("query { %s }", graphQlQuery), expectedCqlQuery, expectedValues);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "TestTable(value: { a: { b: {i:1} } }) { values { a{b{i}} } }",
          "SELECT a FROM udts.\"TestTable\" WHERE a = ?",
          ImmutableList.of(udtA(udtB(1)))),
      arguments(
          "TestTable(filter: { a: {eq: { b: {i:1} } } }) { values { a{b{i}} } }",
          "SELECT a FROM udts.\"TestTable\" WHERE a = ?",
          ImmutableList.of(udtA(udtB(1)))),
      arguments(
          "TestTable(filter: { a: {in: [{ b: {i:1} }, { b: {i:2} }] } }) { values { a{b{i}} } }",
          "SELECT a FROM udts.\"TestTable\" WHERE a IN ?",
          ImmutableList.of(listV(udtA(udtB(1)), udtA(udtB(2))))),
    };
  }

  private static Value udtA(Value udtB) {
    return Values.udtOf(ImmutableMap.of("b", udtB));
  }

  private static Value udtB(int i) {
    return Values.udtOf(ImmutableMap.of("i", Values.of(i)));
  }
}
