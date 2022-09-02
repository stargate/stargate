package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2IndexAddRequest;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
public class RestApiV2QSchemaSAI_IT extends RestApiV2QIntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(RestApiV2QSchemaSAI_IT.class);

  public RestApiV2QSchemaSAI_IT() {
    super("sai_ks_", "sai_t_");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void indexCreateCustomSAI() throws IOException {
    boolean isC4 = IntegrationTestUtils.isCassandra40();
    LOG.info("indexCreateCustomSAI(): is backend Cassandra 4.0? {}", isC4);
    assumeThat(!isC4)
        .as("Disabled because it is currently not possible to enable SAI indexes on C*4.0 backend")
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
