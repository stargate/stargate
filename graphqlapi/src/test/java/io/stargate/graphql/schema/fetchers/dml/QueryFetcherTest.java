package io.stargate.graphql.schema.fetchers.dml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import graphql.ExecutionResult;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.DmlTestBase;
import io.stargate.graphql.schema.SampleKeyspaces;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Execution(ExecutionMode.CONCURRENT)
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

  @ParameterizedTest
  @MethodSource("operationsWithOptions")
  @DisplayName("Should execute GraphQL with options passed correctly")
  public void consistencyAndPagingOptionsTest(
      String graphQlOperation, Parameters expectedParameters) {
    ExecutionResult result = executeGraphQl(graphQlOperation);
    assertThat(result.getErrors()).isEmpty();
    assertThat(parametersCaptor.getValue()).isEqualTo(expectedParameters);
  }

  public static Arguments[] operationsWithOptions() {
    return new Arguments[] {
      arguments(
          "query { books(options: { pageSize: 100, pageState: \"AWEA8H////4A\", consistency: LOCAL_QUORUM }) { values { title, author } } }",
          ImmutableParameters.builder()
              .pageSize(100)
              .pagingState(ByteBuffer.wrap(Base64.getDecoder().decode("AWEA8H////4A")))
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .build()),
      arguments(
          "mutation { insertBooks(value: {title:\"a\", author:\"b\"}, options: { consistency: LOCAL_ONE, serialConsistency: SERIAL}) { applied } }",
          ImmutableParameters.builder()
              .consistencyLevel(ConsistencyLevel.LOCAL_ONE)
              .serialConsistencyLevel(ConsistencyLevel.SERIAL)
              .build())
    };
  }
}
