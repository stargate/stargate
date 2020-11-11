package io.stargate.graphql.schema.fetchers.dml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import graphql.ExecutionResult;
import graphql.schema.GraphQLNamedSchemaElement;
import graphql.schema.GraphQLSchema;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.DmlTestBase;
import io.stargate.graphql.schema.SampleKeyspaces;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryFetcherTest extends DmlTestBase {
  private GraphQLSchema schema = createGraphQlSchema();

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

  @ParameterizedTest
  @MethodSource("typeDescriptions")
  public void typeDescriptionTest(String typeName, String description) {
    GraphQLNamedSchemaElement type = (GraphQLNamedSchemaElement) schema.getType(typeName);
    assertThat(type).isNotNull();
    assertThat(type.getDescription()).isEqualTo(description);
  }

  @ParameterizedTest
  @MethodSource("queryDescriptions")
  public void queryDescriptionTest(String name, String description) {
    assertThat(schema.getQueryType().getFieldDefinition(name).getDescription())
        .isEqualTo(description);
  }

  @ParameterizedTest
  @MethodSource("mutationDescriptions")
  public void mutationDescriptionTest(String name, String description) {
    assertThat(schema.getMutationType().getFieldDefinition(name).getDescription())
        .isEqualTo(description);
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
          "mutation { insertbooks(value: {title:\"a\", author:\"b\"}, options: { consistency: LOCAL_ONE, serialConsistency: SERIAL}) { applied } }",
          ImmutableParameters.builder()
              .consistencyLevel(ConsistencyLevel.LOCAL_ONE)
              .serialConsistencyLevel(ConsistencyLevel.SERIAL)
              .build())
    };
  }

  public static Arguments[] typeDescriptions() {
    return new Arguments[] {
      arguments("Books", "The type used to represent results of a query for the table 'books'."),
      arguments(
          "AuthorsInput",
          "The input type for the table 'authors'.\n"
              + "Note that 'author' and 'title' are the fields that correspond to"
              + " the table primary key."),
      arguments(
          "BooksFilterInput",
          "The input type used for filtering with non-equality operators for the table 'books'.\n"
              + "Note that 'title' is the field that corresponds to the table primary key."),
      arguments(
          "BooksOrder",
          "The enum used to order a query result based on one or more fields for the "
              + "table 'books'."),
      arguments(
          "BooksMutationResult",
          "The type used to represent results of a mutation for the table 'books'."),
      arguments("MutationOptions", "The execution options for the mutation."),
      arguments("QueryOptions", "The execution options for the query."),
    };
  }

  public static Arguments[] queryDescriptions() {
    return new Arguments[] {
      arguments(
          "books",
          "Query for the table 'books'.\n"
              + "Note that 'title' is the field that corresponds to the table primary key."),
      arguments(
          "authors",
          "Query for the table 'authors'.\n"
              + "Note that 'author' and 'title' are the fields that correspond to the"
              + " table primary key."),
    };
  }

  public static Arguments[] mutationDescriptions() {
    return new Arguments[] {
      arguments(
          "insertBooks",
          "Insert mutation for the table 'books'.\n"
              + "Note that 'title' is the field that corresponds to the table primary key."),
      arguments(
          "deleteAuthors",
          "Delete mutation for the table 'authors'.\n"
              + "Note that 'author' and 'title' are the fields that correspond to the table"
              + " primary key."),
      arguments(
          "updateBooks",
          "Update mutation for the table 'books'.\n"
              + "Note that 'title' is the field that corresponds to the table primary key."),
    };
  }
}
