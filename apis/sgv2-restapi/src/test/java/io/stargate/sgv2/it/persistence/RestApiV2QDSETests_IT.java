package io.stargate.sgv2.it.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.it.RestApiV2QCqlEnabledTestBase;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test suite that verifies DSE-specific features.
 *
 * <p>Converted from Stargate V1 test {@code RestApiv2DseTest}.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
public class RestApiV2QDSETests_IT extends RestApiV2QCqlEnabledTestBase {
  public RestApiV2QDSETests_IT() {
    super("dse_ks_", "dse_t_");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  @DisplayName("Should query list column with $contains")
  public void listContainsTest() {
    verifyDSE();
    createLists();

    final String url = endpointPathForRowGetWith(testKeyspaceName(), "lists");
    String response =
        givenWithAuth()
            .queryParam("raw", true)
            .queryParam("where", "{\"l\":{\"$contains\":1}}")
            .when()
            .get(url)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    JsonNode rows = readJsonAsTree(response);
    assertThat(rows).hasSize(1);
    assertThat(rows.at("/0/k").intValue()).isEqualTo(1);

    response =
        givenWithAuth()
            .queryParam("raw", true)
            .queryParam("where", "{\"l\":{\"$contains\":4}}")
            .when()
            .get(url)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    rows = readJsonAsTree(response);
    assertThat(rows).hasSize(0);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private void verifyDSE() {
    assumeThat(IntegrationTestUtils.isDSE()).as("Test only applicable to DSE backend").isTrue();
  }

  private void createLists() {
    final String ks = testKeyspaceName();
    executeCQLs(
        "CREATE TABLE %s.lists(k int PRIMARY KEY, l list<int>)".formatted(ks),
        "CREATE CUSTOM INDEX lists_l_idx ON %s.lists(l) USING 'StorageAttachedIndex'"
            .formatted(ks, ks),
        "INSERT INTO %s.lists (k,l) VALUES (1, [1,2,3])".formatted(ks));
  }

  private void executeCQLs(String... stmts) {
    for (String stmt : stmts) {
      ResultSet resultSet = session.execute(stmt);
      assertThat(resultSet.wasApplied()).isTrue();
    }
  }
}
