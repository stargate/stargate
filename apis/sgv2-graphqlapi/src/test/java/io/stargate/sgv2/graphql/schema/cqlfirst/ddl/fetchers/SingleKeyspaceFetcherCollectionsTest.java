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

public class SingleKeyspaceFetcherCollectionsTest extends DdlTestBase {

  @Override
  protected List<CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.COLLECTIONS);
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected JSON response")
  public void frozenListTest(String query, String expectedJson) {
    assertResponse(query, expectedJson);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "query { "
              + "keyspace(name:\"collections\") {"
              + "    tables {"
              + "      name"
              + "    }"
              + "}"
              + "}",
          "{\"keyspace\":{\"tables\":[{\"name\":\"PkListTable\"},\n"
              + "    {\"name\":\"RegularListTable\"},\n"
              + "    {\"name\":\"PkSetTable\"},\n"
              + "    {\"name\":\"RegularSetTable\"},\n"
              + "    {\"name\":\"PkMapTable\"},\n"
              + "    {\"name\":\"RegularMapTable\"},\n"
              + "    {\"name\":\"NestedCollections\"}]}}"),
      // Frozen list column:
      arguments(
          "query { "
              + "keyspace(name:\"collections\") { "
              + "  table(name:\"PkListTable\") {"
              + "    columns { name, type { basic, info { subTypes {basic}, frozen } } }"
              + "  } } }",
          "{\"keyspace\":{\"table\":{\"columns\":[{"
              + "\"name\": \"l\", "
              + "\"type\":{\"basic\":\"LIST\","
              + "          \"info\":{\"subTypes\":[{\"basic\":\"INT\"}], \"frozen\":true}}"
              + "}]}}}"),
      // Non-frozen list column:
      arguments(
          "query { "
              + "keyspace(name:\"collections\") { "
              + "  table(name:\"RegularListTable\") {"
              + "    columns { name, type { basic, info { subTypes {basic}, frozen } } }"
              + "  } } }",
          "{\"keyspace\":{\"table\":{\"columns\":[{"
              + "\"name\": \"k\", "
              + "\"type\":{\"basic\":\"INT\", \"info\": null}"
              + "}, {"
              + "\"name\": \"l\", "
              + "\"type\":{\"basic\":\"LIST\","
              + "          \"info\":{\"subTypes\":[{\"basic\":\"INT\"}], \"frozen\":false}}"
              + "}]}}}"),
      // Frozen set column:
      arguments(
          "query { "
              + "keyspace(name:\"collections\") { "
              + "  table(name:\"PkSetTable\") {"
              + "    columns { name, type { basic, info { subTypes {basic}, frozen } } }"
              + "  } } }",
          "{\"keyspace\":{\"table\":{\"columns\":[{"
              + "\"name\": \"s\", "
              + "\"type\":{\"basic\":\"SET\","
              + "          \"info\":{\"subTypes\":[{\"basic\":\"INT\"}], \"frozen\":true}}"
              + "}]}}}"),
      // Non-frozen set column:
      arguments(
          "query { "
              + "keyspace(name:\"collections\") { "
              + "  table(name:\"RegularSetTable\") {"
              + "    columns { name, type { basic, info { subTypes {basic}, frozen } } }"
              + "  } } }",
          "{\"keyspace\":{\"table\":{\"columns\":[{"
              + "\"name\": \"k\", "
              + "\"type\":{\"basic\":\"INT\", \"info\": null}"
              + "}, {"
              + "\"name\": \"s\", "
              + "\"type\":{\"basic\":\"SET\","
              + "          \"info\":{\"subTypes\":[{\"basic\":\"INT\"}], \"frozen\":false}}"
              + "}]}}}"),
      // Frozen map column:
      arguments(
          "query { "
              + "keyspace(name:\"collections\") { "
              + "  table(name:\"PkMapTable\") {"
              + "    columns { name, type { basic, info { subTypes {basic}, frozen } } }"
              + "  } } }",
          "{\"keyspace\":{\"table\":{\"columns\":[{"
              + "\"name\": \"m\", "
              + "\"type\":{\"basic\":\"MAP\","
              + "          \"info\":{\"subTypes\":[{\"basic\":\"INT\"},{\"basic\":\"TEXT\"}], \"frozen\":true}}"
              + "}]}}}"),
      // Non-frozen map column:
      arguments(
          "query { "
              + "keyspace(name:\"collections\") { "
              + "  table(name:\"RegularMapTable\") {"
              + "    columns { name, type { basic, info { subTypes {basic}, frozen } } }"
              + "  } } }",
          "{\"keyspace\":{\"table\":{\"columns\":[{"
              + "\"name\": \"k\", "
              + "\"type\":{\"basic\":\"INT\", \"info\": null}"
              + "}, {"
              + "\"name\": \"m\", "
              + "\"type\":{\"basic\":\"MAP\","
              + "          \"info\":{\"subTypes\":[{\"basic\":\"INT\"},{\"basic\":\"TEXT\"}], \"frozen\":false}}"
              + "}]}}}"),
      // Nested collections:
      // Both the query and the result look terrible. This is an edge case that should be rare in
      // practice.
      arguments(
          "query { "
              + "  keyspace(name: \"collections\") { "
              + "    table(name: \"NestedCollections\") { "
              + "      columns { "
              + "        name "
              + "        type { "
              + "          basic "
              + "          info { "
              + "            subTypes { "
              + "              basic "
              + "              info { "
              + "                subTypes { "
              + "                  basic "
              + "                  info { "
              + "                    subTypes { "
              + "                      basic "
              + "}}}}}}}}}}}",
          "{\"keyspace\":{\"table\":{\"columns\":[{"
              + "\"name\":\"k\","
              + "\"type\":{\"basic\":\"INT\", \"info\":null}"
              + "},{"
              + "\"name\":\"c\","
              + "\"type\":{\"basic\":\"MAP\", \"info\":{\"subTypes\":["
              + "    {\"basic\":\"INT\", \"info\":null},\n"
              + "    {\"basic\":\"LIST\", \"info\":{\"subTypes\":["
              + "        {\"basic\":\"SET\", \"info\":{\"subTypes\":["
              + "            {\"basic\":\"TEXT\"}]}}]}}]}}}]}}}"),
    };
  }
}
