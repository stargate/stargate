package io.stargate.sgv2.it.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.it.RestApiV2QCqlEnabledTestBase;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test(s) to verify Materialized View (MV) usage. Separate from other Table/Index tests since MVs
 * not available on all backends.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QMaterializedViewIT extends RestApiV2QCqlEnabledTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(RestApiV2QMaterializedViewIT.class);

  public RestApiV2QMaterializedViewIT() {
    super("mv_ks_", "mv_t_");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void getRowsFromMV() throws Exception {
    boolean isC4 = IntegrationTestUtils.isCassandra40();
    LOG.info("getRowsFromMV(): is backend Cassandra 4.0? {}", isC4);
    assumeThat(isC4)
        .as("Disabled because MVs are not enabled by default on a Cassandra 4 backend")
        .isFalse();

    final String keyspaceName = testKeyspaceName();
    // Let's not use super long default name but instead:
    final String tableName = "tbl_mvread_" + System.currentTimeMillis();
    createTestTable(
        keyspaceName,
        tableName,
        Arrays.asList("id text", "firstName text", "lastName text"),
        Arrays.asList("id"),
        Arrays.asList());

    List<Map<String, String>> expRows =
        insertRows(
            keyspaceName,
            tableName,
            Arrays.asList(
                Arrays.asList("id 1", "firstName John", "lastName Doe"),
                Arrays.asList("id 2", "firstName Sarah", "lastName Smith"),
                Arrays.asList("id 3", "firstName Jane")));
    String materializedViewName = "mv_test_" + System.currentTimeMillis();
    ResultSet resultSet =
        session.execute(
            String.format(
                "CREATE MATERIALIZED VIEW \"%s\".%s "
                    + "AS SELECT id, \"firstName\", \"lastName\" "
                    + "FROM \"%s\".%s "
                    + "WHERE id IS NOT NULL "
                    + "AND \"firstName\" IS NOT NULL "
                    + "AND \"lastName\" IS NOT NULL "
                    + "PRIMARY KEY (id, \"lastName\")",
                keyspaceName, materializedViewName, keyspaceName, tableName));
    assertThat(resultSet.wasApplied()).isTrue();

    // And then read entries using MV:

    // 14-Sep-2022, tatu: Not sure why but there is a transient issue here wrt timing.
    //   Locally test does not appear to ever fail, but in CI it does, with error suggesting
    //   MV has not been created or Schema metadata not (yet) updated. In absence of
    //   better solution, let's try simple wait to give it time; 3 seconds should do it.

    Thread.sleep(3000L);

    List<Map<String, Object>> rows = findAllRowsAsList(keyspaceName, materializedViewName);

    // Alas, due to "id" as partition key, ordering is arbitrary; so need to
    // convert from List to something like Set
    expRows.remove(2); // the MV should only return the rows with a lastName

    assertThat(rows.size()).isEqualTo(2);
    assertThat(new LinkedHashSet<>(rows)).isEqualTo(new LinkedHashSet<>(expRows));
  }
}
