package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.cqlfirst.ddl.DdlTestBase;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SingleKeyspaceFetcherTest extends DdlTestBase {

  @Override
  protected List<CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.LIBRARY);
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected JSON response")
  public void queryTest(String query, String expectedJson) {
    assertResponse(query, expectedJson);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "query { keyspace(name:\"library\") { tables { name } } }",
          "{\"keyspace\":{\"tables\":[{\"name\":\"books\"}, {\"name\":\"authors\"}]}}"),
      arguments(
          "query { keyspace(name:\"library\") { "
              + "  tables1: tables { name } "
              + "  tables2: tables { name } "
              + "} }",
          "{\"keyspace\":{"
              + "\"tables1\":[{\"name\":\"books\"}, {\"name\":\"authors\"}],"
              + "\"tables2\":[{\"name\":\"books\"}, {\"name\":\"authors\"}]"
              + "}}"),
      arguments(
          "query { keyspace(name:\"library\") { "
              + "  books: table(name: \"books\") { columns { name } } "
              + "  authors: table(name: \"authors\") { columns { name } } "
              + "} }",
          "{\"keyspace\":{"
              + "  \"authors\":{\"columns\":[{\"name\":\"author\"}, {\"name\":\"title\"}]}, "
              + "  \"books\":{\"columns\":[{\"name\":\"title\"}, {\"name\":\"author\"}]}"
              + "}}"),
    };
  }
}
