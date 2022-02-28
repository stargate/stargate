package io.stargate.it.http.restapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.ApiServiceExtension;
import io.stargate.it.http.ApiServiceSpec;
import io.stargate.it.http.RestUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for REST API v2 that test Materialized View functionality. Separate from other
 * tests to allow exclusion when testing backends that do not support MVs.
 */
@NotThreadSafe
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec()
@ExtendWith(ApiServiceExtension.class)
@ApiServiceSpec(parametersCustomizer = "buildApiServiceParameters")
public class RestApiv2MVTest extends RestApiTestBase {
  @Test
  public void getAllRowsFromMaterializedView(CqlSession session) throws IOException {
    assumeThat(isCassandra4())
        .as("Disabled because MVs are not enabled by default on a Cassandra 4 backend")
        .isFalse();

    createTestKeyspace(keyspaceName);
    tableName = "tbl_mvread_" + System.currentTimeMillis();
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text", "lastName text"),
        Collections.singletonList("id"),
        null);

    List<Map<String, String>> expRows =
        insertTestTableRows(
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

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s/rows", restUrlBase, keyspaceName, materializedViewName),
            HttpStatus.SC_OK);

    ListOfMapsGetResponseWrapper getResponseWrapper =
        objectMapper.readerFor(ListOfMapsGetResponseWrapper.class).readValue(body);
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);

    // Alas, due to "id" as partition key, ordering is arbitrary; so need to
    // convert from List to something like Set
    List<Map<String, Object>> rows = getResponseWrapper.getData();

    expRows.remove(2); // the MV should only return the rows with a lastName

    assertThat(rows.size()).isEqualTo(2);
    assertThat(new LinkedHashSet<>(rows)).isEqualTo(new LinkedHashSet<>(expRows));
  }
}
