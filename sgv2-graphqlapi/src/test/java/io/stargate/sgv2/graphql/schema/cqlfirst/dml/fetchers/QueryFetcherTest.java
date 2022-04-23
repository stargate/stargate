package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import graphql.ExecutionResult;
import graphql.schema.GraphQLNamedSchemaElement;
import graphql.schema.GraphQLSchema;
import io.stargate.core.util.ByteBufferUtils;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.Consistency;
import io.stargate.proto.QueryOuterClass.ConsistencyValue;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.graphql.schema.CassandraFetcher;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryFetcherTest extends DmlTestBase {

  private final GraphQLSchema schema = createGraphqlSchema();

  @Override
  protected List<CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.LIBRARY);
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlQuery, String expectedCqlQuery, List<Value> expectedValues) {
    assertQuery(String.format("query { %s }", graphQlQuery), expectedCqlQuery, expectedValues);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "books { values { title, author } }",
          "SELECT title, author FROM library.books",
          Collections.emptyList()),
      arguments(
          "books(options: { limit: 10 }) { values { title, author } }",
          "SELECT title, author FROM library.books LIMIT ?",
          ImmutableList.of(Values.of(10))),
      arguments(
          "books(filter: { title: { eq: \"The Road\" } }) { values { title, author } }",
          "SELECT title, author FROM library.books WHERE title = ?",
          ImmutableList.of(Values.of("The Road"))),
      arguments(
          "books(filter: { title: { eq: \"The Road\" } }) { "
              + "  values { title } "
              + "  values2: values { author } "
              + "}",
          "SELECT title, author FROM library.books WHERE title = ?",
          ImmutableList.of(Values.of("The Road"))),
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
      String graphQlOperation, QueryParameters expectedParameters) {
    ExecutionResult result = executeGraphql(graphQlOperation);
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
    QueryParameters defaults = CassandraFetcher.DEFAULT_PARAMETERS;
    return new Arguments[] {
      arguments(
          "query { books(options: { pageSize: 101, pageState: \"AWEA8H////4A\", consistency: LOCAL_QUORUM }) { values { title, author } } }",
          QueryParameters.newBuilder(defaults)
              .setPageSize(Int32Value.of(101))
              .setPagingState(
                  BytesValue.of(ByteString.copyFrom(ByteBufferUtils.fromBase64("AWEA8H////4A"))))
              .setConsistency(
                  ConsistencyValue.newBuilder().setValue(Consistency.LOCAL_QUORUM).build())
              .build()),
      arguments(
          "mutation { insertbooks(value: {title:\"a\", author:\"b\"}, options: { consistency: LOCAL_ONE, serialConsistency: SERIAL}) { applied } }",
          QueryParameters.newBuilder(defaults)
              .setConsistency(ConsistencyValue.newBuilder().setValue(Consistency.LOCAL_ONE).build())
              .setSerialConsistency(
                  ConsistencyValue.newBuilder().setValue(Consistency.SERIAL).build())
              .build()),
      // Verify that the default parameters are pageSize = 100 and cl = LOCAL_QUORUM
      arguments(
          "query { books { values { title, author } } }",
          QueryParameters.newBuilder()
              .setPageSize(Int32Value.of(100))
              .setConsistency(
                  ConsistencyValue.newBuilder().setValue(Consistency.LOCAL_QUORUM).build())
              .setSerialConsistency(
                  ConsistencyValue.newBuilder().setValue(Consistency.SERIAL).build())
              .build()),
      arguments("query { books(options: null) { values { title, author } } }", defaults),
      arguments(
          "mutation { insertbooks(value: {title:\"a\", author:\"b\"}, options: {serialConsistency: LOCAL_SERIAL}) { applied } }",
          QueryParameters.newBuilder(defaults)
              .setSerialConsistency(
                  ConsistencyValue.newBuilder().setValue(Consistency.LOCAL_SERIAL).build())
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
