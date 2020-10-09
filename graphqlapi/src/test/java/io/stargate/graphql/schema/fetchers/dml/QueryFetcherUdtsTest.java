package io.stargate.graphql.schema.fetchers.dml;

import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.DmlTestBase;
import io.stargate.graphql.schema.SampleKeyspaces;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class QueryFetcherUdtsTest extends DmlTestBase {

  @Override
  public Keyspace getKeyspace() {
    return SampleKeyspaces.UDTS;
  }

  @Test
  @DisplayName("Should format UDT value into query")
  public void udtTest() {
    String graphQlQuery =
        "query { testTable(value: { a: { b: { i: 1 } } }) { values { a{b{i}} } } }";
    String expectedCqlQuery = "SELECT a FROM udts.test_table WHERE a={\"b\":{\"i\":1}}";
    assertSuccess(graphQlQuery, expectedCqlQuery);
  }
}
