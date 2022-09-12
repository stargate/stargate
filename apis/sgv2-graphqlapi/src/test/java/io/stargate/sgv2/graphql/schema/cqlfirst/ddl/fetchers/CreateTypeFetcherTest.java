package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.sgv2.graphql.schema.cqlfirst.ddl.DdlTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CreateTypeFetcherTest extends DdlTestBase {

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlMutation, String expectedCqlQuery) {
    assertQuery(String.format("mutation { %s }", graphQlMutation), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "createType(keyspaceName:\"test\", typeName:\"a\", "
              + "fields: [ { name: \"i\", type: {basic: INT} } ])",
          "CREATE TYPE test.a (i int)"),
      arguments(
          "createType(keyspaceName:\"test\", typeName:\"a\", ifNotExists:true, "
              + "fields: [ { name: \"i\", type: {basic: INT} } ])",
          "CREATE TYPE IF NOT EXISTS test.a (i int)"),
      arguments(
          "createType(keyspaceName:\"test\", typeName:\"A\", ifNotExists:true, "
              + "fields: [ { name: \"I\", type: {basic: INT} } ])",
          "CREATE TYPE IF NOT EXISTS test.\"A\" (\"I\" int)"),
    };
  }

  @ParameterizedTest
  @MethodSource("failingQueries")
  @DisplayName("Should execute GraphQL and throw expected error")
  public void errorTest(String graphQlMutation, String expectedError) {
    assertError(String.format("mutation { %s }", graphQlMutation), expectedError);
  }

  public static Arguments[] failingQueries() {
    return new Arguments[] {
      arguments(
          "createType(keyspaceName:\"test\", typeName:\"a\", fields: [])",
          "Must have at least one field"),
    };
  }
}
