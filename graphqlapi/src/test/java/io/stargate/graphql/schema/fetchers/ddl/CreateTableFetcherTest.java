package io.stargate.graphql.schema.fetchers.ddl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import graphql.ExecutionResult;
import graphql.GraphQLError;
import io.stargate.graphql.schema.DdlTestBase;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CreateTableFetcherTest extends DdlTestBase {

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlMutation, String expectedCqlQuery) {
    when(dataStore.query(queryCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(null));

    ExecutionResult result = executeGraphQl(String.format("mutation { %s }", graphQlMutation));
    assertThat(result.getErrors()).isEmpty();
    assertThat(queryCaptor.getValue()).isEqualTo(expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "createTable(keyspaceName:\"library\", tableName:\"books\", "
              + "partitionKeys: [ { name: \"title\", type: {basic: TEXT} } ] "
              + "values: [ { name: \"author\", type: {basic: TEXT} } ])",
          "CREATE TABLE library.books (title text PRIMARY KEY,author text)"),
    };
  }

  @ParameterizedTest
  @MethodSource("failingQueries")
  @DisplayName("Should execute GraphQL and throw expected error")
  public void errorTest(String graphQlMutation, String expectedError) {
    ExecutionResult result = executeGraphQl(String.format("mutation { %s }", graphQlMutation));
    assertThat(result.getErrors()).isNotEmpty();
    GraphQLError error = result.getErrors().get(0);
    assertThat(error.getMessage()).contains(expectedError);
  }

  public static Arguments[] failingQueries() {
    return new Arguments[] {
      arguments(
          "createTable(keyspaceName: \"library\", tableName:\"books\")",
          "Missing field argument partitionKeys @ 'createTable'"),
      arguments(
          "createTable(keyspaceName:\"library\", tableName:\"books\", partitionKeys: [])",
          "partitionKeys must contain at least one element"),
    };
  }
}
