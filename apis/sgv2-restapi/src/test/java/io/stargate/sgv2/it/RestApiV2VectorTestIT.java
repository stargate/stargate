package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2Table;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2VectorTestIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2VectorTestIT() {
    super("vec_ks_", "vec_t_", KeyspaceCreation.PER_CLASS);
  }

  @BeforeAll
  public static void validateRunningOnVSearchEnabled() {
    assumeThat(IntegrationTestUtils.supportsVSearch())
        .as("Test disabled if backend does not support Vector Search (vsearch)")
        .isTrue();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Create with Vector, index
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void tableCreateWithVectorIndex() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id int", "embedding vector<float,5>"),
        Arrays.asList("id"),
        Arrays.asList());

    final Sgv2Table table = findTable(testKeyspaceName(), tableName);

    assertThat(table.keyspace()).isEqualTo(testKeyspaceName());
    assertThat(table.name()).isEqualTo(tableName);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Create, fail
  /////////////////////////////////////////////////////////////////////////
   */

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Create, GET
  /////////////////////////////////////////////////////////////////////////
   */

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Delete
  /////////////////////////////////////////////////////////////////////////
   */
}
