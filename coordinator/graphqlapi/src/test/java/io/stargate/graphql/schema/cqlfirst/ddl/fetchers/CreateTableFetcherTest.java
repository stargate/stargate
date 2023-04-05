package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.cqlfirst.ddl.DdlTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CreateTableFetcherTest extends DdlTestBase {

  @Override
  public Schema getCQLSchema() {
    return Schema.build().keyspace("library").keyspace("ks").build();
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
          "createTable(keyspaceName:\"library\", tableName:\"books\", "
              + "partitionKeys: [ { name: \"title\", type: {basic: TEXT} } ] "
              + "values: [ { name: \"author\", type: {basic: TEXT} } ])",
          "CREATE TABLE library.books (title text, author text, PRIMARY KEY ((title)))"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"l\", type: { basic: LIST, info: { subTypes: [{ basic: INT }], frozen: true } } } ])",
          "CREATE TABLE ks.t (l frozen<list<int>>, PRIMARY KEY ((l)))"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"l\", type: { basic: SET, info: { subTypes: [{ basic: INT }], frozen: true } } } ])",
          "CREATE TABLE ks.t (l frozen<set<int>>, PRIMARY KEY ((l)))"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"m\", type: { basic: MAP, info: { subTypes: ["
              + "  { basic: INT }, { basic: TEXT } "
              + "], frozen: true } } } ])",
          "CREATE TABLE ks.t (m frozen<map<int, text>>, PRIMARY KEY ((m)))"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"k\", type: { basic: UDT, info: { name: \"aType\", frozen: true } } } ])",
          "CREATE TABLE ks.t (k frozen<\"aType\">, PRIMARY KEY ((k)))"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"t\", type: { basic: TUPLE, info: { subTypes: ["
              + "  { basic: INT }, { basic: TEXT }, { basic: FLOAT } "
              + "] } } } ])",
          // Tuples are always frozen; the query builder adds the keyword, even though it's implicit
          "CREATE TABLE ks.t (t frozen<tuple<int, text, float>>, PRIMARY KEY ((t)))"),
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
          "createTable(keyspaceName: \"library\", tableName:\"books\")",
          "Missing field argument partitionKeys @ 'createTable'"),
      arguments(
          "createTable(keyspaceName:\"library\", tableName:\"books\", partitionKeys: [])",
          "partitionKeys must contain at least one element"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"l\", type: { basic: LIST } } ])",
          "List type should contain an 'info' field specifying the sub type"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"l\", type: { basic: LIST, info: {} } } ])",
          "List sub types should contain 1 item"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"l\", type: { basic: LIST, info: { subTypes: [] } } } ])",
          "List sub types should contain 1 item"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"l\", type: { basic: LIST, info: { subTypes: [ { basic: INT }, { basic: TEXT } ] } } } ])",
          "List sub types should contain 1 item"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"l\", type: { basic: SET } } ])",
          "Set type should contain an 'info' field specifying the sub type"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"l\", type: { basic: SET, info: {} } } ])",
          "Set sub types should contain 1 item"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"l\", type: { basic: SET, info: { subTypes: [] } } } ])",
          "Set sub types should contain 1 item"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"l\", type: { basic: SET, info: { subTypes: [ { basic: INT }, { basic: TEXT } ] } } } ])",
          "Set sub types should contain 1 item"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"m\", type: { basic: MAP } } ])",
          "Map type should contain an 'info' field specifying the sub type"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"m\", type: { basic: MAP, info: {} } } ])",
          "Map sub types should contain 2 items"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"m\", type: { basic: MAP, info: { subTypes: [ { basic: INT } ] } } } ])",
          "Map sub types should contain 2 items"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"m\", type: { basic: MAP, info: { subTypes: [ { basic: INT }, { basic: INT }, { basic: INT } ] } } } ])",
          "Map sub types should contain 2 items"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"k\", type: { basic: UDT } } ])",
          "UDT type should contain an 'info' field specifying the UDT name"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"t\", type: { basic: TUPLE } } ])",
          "TUPLE type should contain an 'info' field specifying the sub types"),
      arguments(
          "  createTable(keyspaceName: \"ks\", tableName: \"t\", partitionKeys: ["
              + "{ name: \"t\", type: { basic: TUPLE, info: { subTypes: [] } } } ])",
          "TUPLE type should have at least one sub type"),
    };
  }
}
