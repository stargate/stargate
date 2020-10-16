package io.stargate.graphql.schema.fetchers.dml;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.DmlTestBase;
import io.stargate.graphql.schema.SampleKeyspaces;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryFetcherCollectionsTest extends DmlTestBase {

  @Override
  public Keyspace getKeyspace() {
    return SampleKeyspaces.COLLECTIONS;
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL with collections and generate expected CQL query")
  public void collectionsTest(String graphQlQuery, String expectedCqlQuery) {
    assertSuccess(String.format("query { %s }", graphQlQuery), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    // Note that some of these queries require ALLOW FILTERING or indexes in real life. It doesn't
    // matter here, we're just testing the generation of the query, not executing it.
    return new Arguments[] {
      // List:
      arguments(
          "pkListTable(value: { l: [1,2,3] }) { values { l } }",
          "SELECT l FROM collections.pk_list_table WHERE l=[1,2,3]"),
      arguments(
          "pkListTable(filter: { l: {eq: [1,2,3] } }) { values { l } }",
          "SELECT l FROM collections.pk_list_table WHERE l=[1,2,3]"),
      arguments(
          "pkListTable(filter: { l: {gt: [1,2,3] } }) { values { l } }",
          "SELECT l FROM collections.pk_list_table WHERE l>[1,2,3]"),
      arguments(
          "pkListTable(filter: { l: {gte: [1,2,3] } }) { values { l } }",
          "SELECT l FROM collections.pk_list_table WHERE l>=[1,2,3]"),
      arguments(
          "pkListTable(filter: { l: {lt: [1,2,3] } }) { values { l } }",
          "SELECT l FROM collections.pk_list_table WHERE l<[1,2,3]"),
      arguments(
          "pkListTable(filter: { l: {lte: [1,2,3] } }) { values { l } }",
          "SELECT l FROM collections.pk_list_table WHERE l<=[1,2,3]"),
      arguments(
          "pkListTable(filter: { l: {in: [[1,2,3],[4,5,6]] } }) { values { l } }",
          "SELECT l FROM collections.pk_list_table WHERE l IN ([1,2,3],[4,5,6])"),
      arguments(
          "regularListTable(filter: { l: {contains: 1 } }) { values { l } }",
          "SELECT l FROM collections.regular_list_table WHERE l CONTAINS 1"),

      // Set:
      arguments(
          "pkSetTable(value: { s: [1,2,3] }) { values { s } }",
          "SELECT s FROM collections.pk_set_table WHERE s={1,2,3}"),
      arguments(
          "pkSetTable(filter: { s: {eq: [1,2,3] } }) { values { s } }",
          "SELECT s FROM collections.pk_set_table WHERE s={1,2,3}"),
      arguments(
          "pkSetTable(filter: { s: {gt: [1,2,3] } }) { values { s } }",
          "SELECT s FROM collections.pk_set_table WHERE s>{1,2,3}"),
      arguments(
          "pkSetTable(filter: { s: {gte: [1,2,3] } }) { values { s } }",
          "SELECT s FROM collections.pk_set_table WHERE s>={1,2,3}"),
      arguments(
          "pkSetTable(filter: { s: {lt: [1,2,3] } }) { values { s } }",
          "SELECT s FROM collections.pk_set_table WHERE s<{1,2,3}"),
      arguments(
          "pkSetTable(filter: { s: {lte: [1,2,3] } }) { values { s } }",
          "SELECT s FROM collections.pk_set_table WHERE s<={1,2,3}"),
      arguments(
          "pkSetTable(filter: { s: {in: [[1,2,3],[4,5,6]] } }) { values { s } }",
          "SELECT s FROM collections.pk_set_table WHERE s IN ({1,2,3},{4,5,6})"),
      arguments(
          "regularSetTable(filter: { s: {contains: 1 } }) { values { s } }",
          "SELECT s FROM collections.regular_set_table WHERE s CONTAINS 1"),

      // Map:
      arguments(
          "pkMapTable(value: { m: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] }) { values { m{key,value} } }",
          "SELECT m FROM collections.pk_map_table WHERE m={1:'a',2:'b'}"),
      arguments(
          "pkMapTable(filter: { m: { eq: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.pk_map_table WHERE m={1:'a',2:'b'}"),
      arguments(
          "pkMapTable(filter: { m: { gt: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.pk_map_table WHERE m>{1:'a',2:'b'}"),
      arguments(
          "pkMapTable(filter: { m: { gte: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.pk_map_table WHERE m>={1:'a',2:'b'}"),
      arguments(
          "pkMapTable(filter: { m: { lt: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.pk_map_table WHERE m<{1:'a',2:'b'}"),
      arguments(
          "pkMapTable(filter: { m: { lte: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.pk_map_table WHERE m<={1:'a',2:'b'}"),
      arguments(
          "pkMapTable(filter: { m: { in: ["
              + "  [{key: 1,value:\"a\"},{key: 2,value:\"b\"}],"
              + "  [{key: 3,value:\"c\"},{key: 4,value:\"d\"}]"
              + "] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.pk_map_table WHERE m IN ({1:'a',2:'b'},{3:'c',4:'d'})"),
      arguments(
          "regularMapTable(filter: { m: {containsKey: 1 } }) { values { m{key,value} } }",
          "SELECT m FROM collections.regular_map_table WHERE m CONTAINS KEY 1"),
      arguments(
          "regularMapTable(filter: { m: {containsValue: \"a\" } }) { values { m{key,value} } }",
          "SELECT m FROM collections.regular_map_table WHERE m CONTAINS 'a'"),
      arguments(
          "regularMapTable(filter: { m: {containsEntry: {key: 1,value:\"a\"} } }) { values { m{key,value} } }",
          "SELECT m FROM collections.regular_map_table WHERE m[1]='a'"),

      // Nested collection (map<int, list<set<text>>>):
      arguments(
          "nestedCollections(filter: { c: { eq: ["
              + "  {key: 1, value:[[\"a\"],[\"b\"]]},"
              + "  {key: 2, value:[[\"c\"],[\"d\"]]}"
              + "] } }) "
              + "{ values { k } }",
          "SELECT k FROM collections.nested_collections WHERE c={1:[{'a'},{'b'}],2:[{'c'},{'d'}]}"),
      arguments(
          "nestedCollections(filter: { c: { containsValue: [[\"a\"],[\"b\"]] } })"
              + "{ values { k } }",
          "SELECT k FROM collections.nested_collections WHERE c CONTAINS [{'a'},{'b'}]"),
      arguments(
          "nestedCollections(filter: { c: { containsEntry: {key: 1, value:[[\"a\"],[\"b\"]]} } })"
              + "{ values { k } }",
          "SELECT k FROM collections.nested_collections WHERE c[1]=[{'a'},{'b'}]"),
    };
  }
}
