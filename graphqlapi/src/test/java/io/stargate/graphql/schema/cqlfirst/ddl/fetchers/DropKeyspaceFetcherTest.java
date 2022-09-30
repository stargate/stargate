package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.cqlfirst.ddl.DdlTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DropKeyspaceFetcherTest extends DdlTestBase {

  @Override
  public Schema getCQLSchema() {
    return Schema.build().keyspace("test").keyspace("Test").build();
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlMutation, String expectedCqlQuery) {
    assertQuery(String.format("mutation { %s }", graphQlMutation), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments("dropKeyspace(name:\"test\")", "DROP KEYSPACE test"),
      arguments("dropKeyspace(name:\"Test\")", "DROP KEYSPACE \"Test\""),
      arguments("dropKeyspace(name:\"test\",ifExists:true)", "DROP KEYSPACE IF EXISTS test"),
    };
  }
}
