package io.stargate.it.http.restapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.RestUtils;
import io.stargate.web.restapi.models.ColumnDefinition;
import io.stargate.web.restapi.models.PrimaryKey;
import io.stargate.web.restapi.models.TableAdd;
import io.stargate.web.restapi.models.TableResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
public class RestApiv2MVTest extends RestApiTestBase {

  /*
  /************************************************************************
  /* Test methods
  /************************************************************************
   */

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

  /*
  /************************************************************************
  /* Helper methods for setting up tests
  /************************************************************************
   */

  private void createTestTable(
      String tableName, List<String> columns, List<String> partitionKey, List<String> clusteringKey)
      throws IOException {
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions =
        columns.stream()
            .map(x -> x.split(" "))
            .map(y -> new ColumnDefinition(y[0], y[1]))
            .collect(Collectors.toList());
    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(partitionKey);
    if (clusteringKey != null) {
      primaryKey.setClusteringKey(clusteringKey);
    }
    tableAdd.setPrimaryKey(primaryKey);

    String body =
        RestUtils.post(
            authToken,
            String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
            objectMapper.writeValueAsString(tableAdd),
            HttpStatus.SC_CREATED);

    TableResponse tableResponse = objectMapper.readValue(body, TableResponse.class);
    assertThat(tableResponse.getName()).isEqualTo(tableName);
  }

  /**
   * Helper method for inserting table entries using so-called "Stringified" values for columns:
   * this differs a bit from full JSON values and is mostly useful for simple String and number
   * fields.
   *
   * @return {@code List} of entries to expect back for given definitions.
   */
  private List<Map<String, String>> insertTestTableRows(List<List<String>> rows)
      throws IOException {
    final List<Map<String, String>> insertedRows = new ArrayList<>();
    for (List<String> row : rows) {
      Map<String, String> rowMap = new HashMap<>();
      for (String kv : row) {
        // Split on first space, leave others in (with no limit we'd silently
        // drop later space-separated parts)
        String[] parts = kv.split(" ", 2);
        rowMap.put(parts[0].trim(), parts[1].trim());
      }
      insertedRows.add(rowMap);

      RestUtils.post(
          authToken,
          String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
          objectMapper.writeValueAsString(rowMap),
          HttpStatus.SC_CREATED);
    }
    return insertedRows;
  }
}
