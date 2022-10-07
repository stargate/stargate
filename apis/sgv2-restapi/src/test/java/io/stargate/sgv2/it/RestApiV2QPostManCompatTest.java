package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.it.RestApiV2QIntegrationTestBase;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.UUID;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
public class RestApiV2QPostManCompatTest extends RestApiV2QIntegrationTestBase {
  public RestApiV2QPostManCompatTest() {
    super("postman_ks_", "postman_t_");
  }

  @TestHTTPResource("/v2/keyspaces")
  URL urlKeyspaces;

  // for [stargate#2142]: failure observed via Postman, related to it only escaping some
  // of characters it should (double-quotes yes, curly braces/brackets)
  @Test
  public void getRowsWithQueryNoEscapeForJSON() throws Exception {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final String rowIdentifier = UUID.randomUUID().toString();
    insertTypedRows(
        testKeyspaceName(),
        tableName,
        Arrays.asList(map("id", rowIdentifier, "firstName", "John")));

    // Alas, RestAssured cannot pass badly escaped Query Parameters, need to revert to
    // using JDK HttpClient or similar

    // Let's only escape double-quotes, similar to Postman
    String whereClause = "{%22id%22:{%22$eq%22:%22" + rowIdentifier + "%22}}";

    // This actually works, if uncommented:
//    String whereClause = "%7b%22id%22:%7b%22$eq%22:%22" + rowIdentifier + "%22%7d%7d";

    final String urlStr =
        urlKeyspaces.toString()
            + String.format("/%s/%s?raw=true&where=%s", testKeyspaceName(), tableName, whereClause);

    HttpClient client = HttpClient.newBuilder().build();
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(new URL(urlStr).toURI())
            .header(
                HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
            .header("Content-Type", "application/json")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(HttpStatus.SC_OK);
    String body = response.body();

    JsonNode rows = readJsonAsTree(body);
    assertThat(rows).hasSize(1);
    assertThat(rows.at("/0/id").asText()).isEqualTo(rowIdentifier);
    assertThat(rows.at("/0/firstName").asText()).isEqualTo("John");
  }
}
