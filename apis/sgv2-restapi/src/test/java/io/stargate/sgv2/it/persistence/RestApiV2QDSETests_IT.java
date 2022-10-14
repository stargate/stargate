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

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
  // Test methods for Collections (Lists, Sets)
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  @DisplayName("Should query list column with $contains")
  public void listContainsTest() {
    verifyDSE();

    final String ks = testKeyspaceName();
    executeCQLs(
            "CREATE TABLE %s.lists(k int PRIMARY KEY, l list<int>)".formatted(ks),
            "CREATE CUSTOM INDEX lists_l_idx ON %s.lists(l) USING 'StorageAttachedIndex'"
                    .formatted(ks, ks),
            "INSERT INTO %s.lists (k,l) VALUES (1, [1,2,3])".formatted(ks));

    final String url = endpointPathForRowGetWith(ks, "lists");
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

  @Test
  @DisplayName("Should query set column with $contains")
  public void setContainsTest() {
    verifyDSE();

    final String ks = testKeyspaceName();
    executeCQLs(
            "CREATE TABLE %s.sets(k int PRIMARY KEY, s set<int>)".formatted(ks),
            "CREATE CUSTOM INDEX sets_s_idx ON %s.sets(s) USING 'StorageAttachedIndex'".formatted(ks),
            "INSERT INTO %s.sets (k,s) VALUES (1, {1,2,3})".formatted(ks)
    );

    final String url = endpointPathForRowGetWith(ks, "sets");
    String response =
            givenWithAuth()
                    .queryParam("raw", true)
                    .queryParam("where", "{\"s\":{\"$contains\":1}}")
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
                    .queryParam("where", "{\"s\":{\"$contains\":4}}")
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
  // Test methods for Maps
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  @DisplayName("Should query map column with $containsKey")
  public void mapContainsKeyTest() {
    verifyDSE();

    final String ks = testKeyspaceName();
    executeCQLs(
            // Map, indexed by key
            "CREATE TABLE %s.maps_per_key(k int PRIMARY KEY, m map<int, text>)".formatted(ks),
            "CREATE CUSTOM INDEX maps_per_key_m_idx ON %s.maps_per_key(keys(m)) USING 'StorageAttachedIndex'".formatted(ks),
            "INSERT INTO %s.maps_per_key (k,m) values (1, {1:'a',2:'b',3:'c'})".formatted(ks)
    );
  }

  @Test
  @DisplayName("Should query map column with $contains")
  public void mapContainsTest() {
    verifyDSE();

    final String ks = testKeyspaceName();
    executeCQLs(
            // Map, indexed by value
            "CREATE TABLE %s.maps_per_value(k int PRIMARY KEY, m map<int, text>)".formatted(ks),
            "CREATE CUSTOM INDEX maps_per_value_m_idx ON %s.maps_per_value(m) USING 'StorageAttachedIndex'".formatted(ks),
            "INSERT INTO %s.maps_per_value (k,m) values (1, {1:'a',2:'b',3:'c'})".formatted(ks)
            );
  }

  @Test
  @DisplayName("Should query map column with $containsEntry")
  public void mapContainsEntryTest() {
    verifyDSE();

    final String ks = testKeyspaceName();
    executeCQLs(
            // Map, indexed by entry
            "CREATE TABLE %s.maps_per_entry(k int PRIMARY KEY, m map<int, text>)".formatted(ks),
            "CREATE CUSTOM INDEX maps_per_entry_m_idx ON %s.maps_per_entry(entries(m)) USING 'StorageAttachedIndex'".formatted(ks),
            "INSERT INTO %s.maps_per_entry (k,m) values (1, {1:'a',2:'b',3:'c'})".formatted(ks)
            );
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods for Indexes
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  @DisplayName("Should be able to create custom indexes with 'StorageAttachedIndex'")
  public void createCustomIndexes() {
    // Start by creating test table needed
    final String ks = testKeyspaceName();
    executeCQLs(
            "CREATE TABLE %s.index_test_table(k int PRIMARY KEY, l list<int>, m1 map<int, text>, m2 map<int, text>, m3 map<int, text>)"
                    .formatted(ks)
    );
  }


  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private void verifyDSE() {
    assumeThat(IntegrationTestUtils.isDSE()).as("Test only applicable to DSE backend").isTrue();
  }

  private void executeCQLs(String... stmts) {
    for (String stmt : stmts) {
      ResultSet resultSet = session.execute(stmt);
      assertThat(resultSet.wasApplied()).isTrue();
    }
  }
}
