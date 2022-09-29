package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.cqlfirst.ddl.DdlTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CreateIndexFetcherTest extends DdlTestBase {

  @Override
  public Schema getCQLSchema() {
    return Schema.build()
        .keyspace("library")
        .table("sales")
        .column("id", Column.Type.Text, Column.Kind.PartitionKey)
        .column("title", Column.Type.Text, Column.Kind.Regular)
        .column("ranking", Column.Type.Map.of(Column.Type.Int, Column.Type.Text))
        .column("prices", Column.Type.List.of(Column.Type.Double).frozen(true))
        .build();
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlMutation, String expectedCqlQuery) {
    assertQuery(String.format("mutation { %s }", graphQlMutation), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"title\")",
          "CREATE INDEX ON library.sales (title)"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"title\", indexName:\"test_idx\")",
          "CREATE INDEX test_idx ON library.sales (title)"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"title\", indexName:\"test_idx\", ifNotExists: true)",
          "CREATE INDEX IF NOT EXISTS test_idx ON library.sales (title)"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"title\", indexType:\"org.apache.cassandra.index.sasi.SASIIndex\")",
          "CREATE CUSTOM INDEX ON library.sales (title) USING 'org.apache.cassandra.index.sasi.SASIIndex'"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"ranking\", indexKind: KEYS)",
          "CREATE INDEX ON library.sales (KEYS(ranking))"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"ranking\", indexKind: ENTRIES)",
          "CREATE INDEX ON library.sales (ENTRIES(ranking))"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"ranking\", indexKind: VALUES)",
          "CREATE INDEX ON library.sales (VALUES(ranking))"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"prices\", indexKind: FULL)",
          "CREATE INDEX ON library.sales (FULL(prices))"),
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
          "createIndex(tableName:\"sales\", columnName:\"title\")",
          "Missing field argument keyspaceName @ 'createIndex'"),
      arguments(
          "createIndex(keyspaceName:\"library\", columnName:\"title\")",
          "Missing field argument tableName @ 'createIndex'"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\")",
          "Missing field argument columnName @ 'createIndex'"),
    };
  }
}
