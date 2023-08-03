package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restapi.service.models.Sgv2Table;
import io.stargate.sgv2.restapi.service.models.Sgv2TableAddRequest;
import java.util.Arrays;
import java.util.List;
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
    createVectorTable(testKeyspaceName(), tableName);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Create, fail
  /////////////////////////////////////////////////////////////////////////
   */

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Create, GET Row(s)
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void insertRowWithVectorValue() {
    final String tableName = testTableName();
    createVectorTable(testKeyspaceName(), tableName);

    insertTypedRows(
        testKeyspaceName(),
        tableName,
        Arrays.asList(
            map("id", 1, "embedding", Arrays.asList(0.0, 0.0, 0.25, 0.0, 0.0)),
            map("id", 2, "embedding", Arrays.asList(0.5, 0.5, 0.5, 0.5, 0.5)),
            map("id", 3, "embedding", Arrays.asList(1.0, 1.0, 1.0, 1.0, 0.875))));
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Delete
  /////////////////////////////////////////////////////////////////////////
   */

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private void createVectorTable(String keyspace, String tableName) {
    final List<Sgv2ColumnDefinition> columnDefs =
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "int", false),
            new Sgv2ColumnDefinition("embedding", "vector<float, 5>", false));
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey(Arrays.asList("id"), null);
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);
    NameResponse response = createTable(keyspace, tableAdd);
    assertThat(response.name).isEqualTo(tableName);

    // And then find the table itself
    final Sgv2Table table = findTable(keyspace, tableName);
    assertThat(table.name()).isEqualTo(tableName);

    assertThat(table.columnDefinitions()).hasSize(2);
    assertThat(table.columnDefinitions()).containsExactlyElementsOf(columnDefs);

    // Plus then SAI for vector field too
    createTestIndex(testKeyspaceName(), tableName, "embedding", "embedding_idx", true, null);
  }
}
