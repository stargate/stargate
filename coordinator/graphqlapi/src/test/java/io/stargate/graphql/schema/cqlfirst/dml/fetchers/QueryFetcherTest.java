package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import graphql.ExecutionResult;
import graphql.schema.GraphQLNamedSchemaElement;
import graphql.schema.GraphQLSchema;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.schema.SampleKeyspaces;
import io.stargate.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryFetcherTest extends DmlTestBase {
  private final GraphQLSchema schema = createGraphQlSchema();

  @Override
  public Schema getCQLSchema() {
    return Schema.create(Collections.singleton(SampleKeyspaces.LIBRARY));
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlQuery, String expectedCqlQuery) {
    assertQuery(String.format("query { %s }", graphQlQuery), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments("books { values { title, author } }", "SELECT title, author FROM library.books"),
      arguments(
          "books(options: { limit: 10 }) { values { title, author } }",
          "SELECT title, author FROM library.books LIMIT 10"),
      arguments(
          "books(filter: { title: { eq: \"The Road\" } }) { values { title, author } }",
          "SELECT title, author FROM library.books WHERE title = 'The Road'"),
      arguments(
          "books(filter: { title: { eq: \"The Road\" } }) { "
              + "  values { title } "
              + "  values2: values { author } "
              + "}",
          "SELECT title, author FROM library.books WHERE title = 'The Road'"),
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
    assertThat(getCapturedParameters()).isEqualTo(expectedParameters);
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
    Parameters defaults = CassandraFetcher.DEFAULT_PARAMETERS;
    return new Arguments[] {
      arguments(
          "query { books(options: { pageSize: 101, pageState: \"AWEA8H////4A\", consistency: LOCAL_QUORUM }) { values { title, author } } }",
          ImmutableParameters.builder()
              .from(defaults)
              .pageSize(101)
              .pagingState(ByteBuffer.wrap(Base64.getDecoder().decode("AWEA8H////4A")))
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .build()),
      arguments(
          "mutation { insertbooks(value: {title:\"a\", author:\"b\"}, options: { consistency: LOCAL_ONE, serialConsistency: SERIAL}) { applied } }",
          ImmutableParameters.builder()
              .from(defaults)
              .consistencyLevel(ConsistencyLevel.LOCAL_ONE)
              .serialConsistencyLevel(ConsistencyLevel.SERIAL)
              .build()),
      // Verify that the default parameters are pageSize = 100 and cl = LOCAL_QUORUM
      arguments(
          "query { books { values { title, author } } }",
          ImmutableParameters.builder()
              .pageSize(100)
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .serialConsistencyLevel(ConsistencyLevel.SERIAL)
              .build()),
      arguments("query { books(options: null) { values { title, author } } }", defaults),
      arguments(
          "mutation { insertbooks(value: {title:\"a\", author:\"b\"}, options: {serialConsistency: LOCAL_SERIAL}) { applied } }",
          ImmutableParameters.builder()
              .from(defaults)
              .serialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
              .build()),
      arguments(
          "mutation { insertbooks(value: {title:\"a\", author:\"b\"}, options: null) { applied } }",
          defaults),
      arguments(
          "mutation { insertbooks(value: {title:\"a\", author:\"b\"} ) { applied } }", defaults),
    };
  }

  public static Arguments[] typeDescriptions() {
    return new Arguments[] {
      arguments("books", "The type used to represent results of a query for the table 'books'."),
      arguments(
          "authorsInput",
          "The input type for the table 'authors'.\n"
              + "Note that 'author' and 'title' are the fields that correspond to"
              + " the table primary key."),
      arguments(
          "booksFilterInput",
          "The input type used for filtering with non-equality operators for the table 'books'.\n"
              + "Note that 'title' is the field that corresponds to the table primary key."),
      arguments(
          "booksOrder",
          "The enum used to order a query result based on one or more fields for the "
              + "table 'books'."),
      arguments(
          "booksMutationResult",
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
          "insertbooks",
          "Insert mutation for the table 'books'.\n"
              + "Note that 'title' is the field that corresponds to the table primary key."),
      arguments(
          "deleteauthors",
          "Delete mutation for the table 'authors'.\n"
              + "Note that 'author' and 'title' are the fields that correspond to the table"
              + " primary key."),
      arguments(
          "updatebooks",
          "Update mutation for the table 'books'.\n"
              + "Note that 'title' is the field that corresponds to the table primary key."),
    };
  }
}
