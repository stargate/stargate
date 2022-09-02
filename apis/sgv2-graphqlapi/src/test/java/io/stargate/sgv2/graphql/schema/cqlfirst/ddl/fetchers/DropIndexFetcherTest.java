package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.sgv2.graphql.schema.cqlfirst.ddl.DdlTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DropIndexFetcherTest extends DdlTestBase {

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlMutation, String expectedCqlQuery) {
    assertQuery(String.format("mutation { %s }", graphQlMutation), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "dropIndex(keyspaceName:\"library\", indexName:\"books_author_idx\")",
          "DROP INDEX library.books_author_idx"),
      arguments(
          "dropIndex(keyspaceName:\"library\", indexName:\"books_author_idx\", ifExists: true)",
          "DROP INDEX IF EXISTS library.books_author_idx")
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
          "dropIndex(keyspaceName:\"library\")", "Missing field argument indexName @ 'dropIndex'"),
      arguments(
          "dropIndex(indexName:\"books_author_idx\")",
          "Missing field argument keyspaceName @ 'dropIndex'"),
    };
  }
}
