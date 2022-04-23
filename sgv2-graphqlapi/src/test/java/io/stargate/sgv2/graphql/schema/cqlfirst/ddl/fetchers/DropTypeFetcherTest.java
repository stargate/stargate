package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.sgv2.graphql.schema.cqlfirst.ddl.DdlTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DropTypeFetcherTest extends DdlTestBase {

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlMutation, String expectedCqlQuery) {
    assertQuery(String.format("mutation { %s }", graphQlMutation), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments("dropType(keyspaceName:\"test\", typeName:\"a\")", "DROP TYPE test.a"),
      arguments(
          "dropType(keyspaceName:\"test\", typeName:\"a\", ifExists:true)",
          "DROP TYPE IF EXISTS test.a"),
      arguments("dropType(keyspaceName:\"Test\", typeName:\"A\")", "DROP TYPE \"Test\".\"A\""),
    };
  }
}
