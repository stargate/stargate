package io.stargate.graphql.schema.fetchers.dml;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.DmlTestBase;
import io.stargate.graphql.schema.SampleKeyspaces;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryFetcherTest extends DmlTestBase {

  @Override
  public Keyspace getKeyspace() {
    return SampleKeyspaces.LIBRARY;
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlQuery, String expectedCqlQuery) {
    assertSuccess(String.format("query { %s }", graphQlQuery), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments("books { values { title, author } }", "SELECT title,author FROM library.books"),
      arguments(
          "books(options: { limit: 10 }) { values { title, author } }",
          "SELECT title,author FROM library.books LIMIT 10"),
      arguments(
          "books(filter: { title: { eq: \"The Road\" } }) { values { title, author } }",
          "SELECT title,author FROM library.books WHERE title='The Road'"),
    };
  }

  @ParameterizedTest
  @MethodSource("failingQueries")
  @DisplayName("Should execute GraphQL and throw expected error")
  public void errorTest(String graphQlMutation, String expectedError) {
    assertError(String.format("query { %s }", graphQlMutation), expectedError);
  }

  public static Arguments[] failingQueries() {
    return new Arguments[] {
      arguments(
          "books(options: { limit: 1.0 }) { values { title } }",
          "argument 'options.limit' with value 'FloatValue{value=1.0}' is not a valid 'Int'"),
    };
  }
}
