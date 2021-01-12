package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.SampleKeyspaces;
import io.stargate.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryFetcherUdtsTest extends DmlTestBase {

  @Override
  public Schema getCQLSchema() {
    return Schema.create(Collections.singleton(SampleKeyspaces.UDTS));
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL with UDTs and generate expected CQL query")
  public void udtTest(String graphQlQuery, String expectedCqlQuery) {
    assertQuery(String.format("query { %s }", graphQlQuery), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "TestTable(value: { a: { b: {i:1} } }) { values { a{b{i}} } }",
          "SELECT a FROM udts.\"TestTable\" WHERE a = {b:{i:1}}"),
      arguments(
          "TestTable(filter: { a: {eq: { b: {i:1} } } }) { values { a{b{i}} } }",
          "SELECT a FROM udts.\"TestTable\" WHERE a = {b:{i:1}}"),
      arguments(
          "TestTable(filter: { a: {in: [{ b: {i:1} }, { b: {i:2} }] } }) { values { a{b{i}} } }",
          "SELECT a FROM udts.\"TestTable\" WHERE a IN ({b:{i:1}}, {b:{i:2}})"),
    };
  }
}
