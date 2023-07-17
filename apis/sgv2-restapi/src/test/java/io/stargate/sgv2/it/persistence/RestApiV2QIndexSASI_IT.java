package io.stargate.sgv2.it.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.it.RestApiV2QIntegrationTestBase;
import io.stargate.sgv2.restapi.service.models.Sgv2IndexAddRequest;
import java.util.Arrays;
import java.util.Collections;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test(s) to verify SSTable-Attached Secondary Index (SASI) creation. Separate from other Index
 * tests since SASI not available on all backends.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QIndexSASI_IT extends RestApiV2QIntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(RestApiV2QIndexSASI_IT.class);

  public RestApiV2QIndexSASI_IT() {
    super("sasi_ks_", "sasi_t_", KeyspaceCreation.PER_CLASS);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void indexCreateCustomSASI() {
    assumeThat(IntegrationTestUtils.supportsSASI())
        .as("Disabled because SASI not enabled by default on Cassandra backend")
        .isTrue();

    final String tableName = testTableName();
    final String indexName = "test_custom_idx";
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "firstName text", "lastName text", "email list<text>"),
        Arrays.asList("id"),
        Arrays.asList());

    Sgv2IndexAddRequest indexAdd = new Sgv2IndexAddRequest("lastName", indexName);
    String indexType = "org.apache.cassandra.index.sasi.SASIIndex";

    indexAdd.setType(indexType);
    indexAdd.setIfNotExists(false);
    indexAdd.setKind(null);

    indexAdd.setOptions(Collections.singletonMap("mode", "CONTAINS"));

    String response =
        tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_CREATED);
    IndexResponse successResponse = readJsonAs(response, IndexResponse.class);
    assertThat(successResponse.success).isTrue();

    // And then verify it's in there actually
    JsonNode indexes = findIndexRows();
    assertThat(indexes.isArray()).isTrue();
    JsonNode row = null;
    for (JsonNode node : indexes) {
      if (indexName.equals(node.path("index_name").asText())) {
        row = node;
        break;
      }
    }
    assertThat(row)
        .withFailMessage("Could not find index '%s' from '%s'", indexName, indexes)
        .isNotNull();
    JsonNode options = row.path("options");
    assertThat(options.path("class_name").asText()).isEqualTo(indexType);
    assertThat(options.path("target").asText()).isEqualTo("\"lastName\"");
    assertThat(options.path("mode").asText()).isEqualTo("CONTAINS");
  }

  private JsonNode findIndexRows() {
    final String path = endpointPathForAllRows("system_schema", "indexes");
    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAsTree(response);
  }
}
