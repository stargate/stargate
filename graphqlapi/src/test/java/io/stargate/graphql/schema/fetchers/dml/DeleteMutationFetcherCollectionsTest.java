package io.stargate.graphql.schema.fetchers.dml;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.DmlTestBase;
import io.stargate.graphql.schema.SampleKeyspaces;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DeleteMutationFetcherCollectionsTest extends DmlTestBase {

  @Override
  public Keyspace getKeyspace() {
    return SampleKeyspaces.COLLECTIONS;
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL with collections and generate expected CQL query")
  public void collectionsTest(String graphQlQuery, String expectedCqlQuery) {
    assertSuccess(String.format("mutation { %s }", graphQlQuery), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "deleteRegularListTable(value: {k: 1}, ifCondition: { l: {notEq: [1,2,3] } }) { applied }",
          "DELETE FROM collections.regular_list_table WHERE k=1 IF l!=[1,2,3]"),
      arguments(
          "deleteRegularSetTable(value: {k: 1}, ifCondition: { s: {notEq: [1,2,3] } }) { applied }",
          "DELETE FROM collections.regular_set_table WHERE k=1 IF s!={1,2,3}"),
      arguments(
          "deleteRegularMapTable(value: {k: 1},"
              + "  ifCondition: { m: {notEq: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) "
              + "{ applied }",
          "DELETE FROM collections.regular_map_table WHERE k=1 IF m!={1:'a',2:'b'}"),
    };
  }
}
