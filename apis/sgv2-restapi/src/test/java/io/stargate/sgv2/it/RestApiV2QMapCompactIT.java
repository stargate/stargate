package io.stargate.sgv2.it;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QMapCompactIT extends RestApiV2QIntegrationTestBase
    implements RestApiV2QMapTests {
  /**
   * Flag to indicate if the default MAP data type is compact or not. We are not explicitly setting
   * this flag here, because the default value is true as per {@link
   * io.stargate.sgv2.restapi.config.RestApiConstants#COMPACT_MAP_DATA}
   */
  private static final boolean COMPACT_MAP_DATA = true;

  public RestApiV2QMapCompactIT() {
    super("maptest_ks_", "maptest_t_", KeyspaceCreation.PER_CLASS);
  }

  @Test
  @Override
  public void addRowWithCompactMap() {
    RestApiV2QMapTestsImplIT.addRowWithCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.addRowWithCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void addRowWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.addRowWithNonCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.addRowWithNonCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void updateRowWithCompactMap() {
    RestApiV2QMapTestsImplIT.updateRowWithCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.updateRowWithCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void updateRowWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.updateRowWithNonCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.updateRowWithNonCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void patchRowWithCompactMap() {
    RestApiV2QMapTestsImplIT.patchRowWithCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.patchRowWithCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void patchRowWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.patchRowWithNonCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.patchRowWithNonCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void getRowsWithCompactMap() {
    RestApiV2QMapTestsImplIT.getRowsWithCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.getRowsWithCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void getRowsWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.getRowsWithNonCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.getRowsWithNonCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void getAllRowsWithCompactMap() {
    RestApiV2QMapTestsImplIT.getAllRowsWithCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.getAllRowsWithCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void getAllRowsWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.getAllRowsWithNonCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.getAllRowsWithNonCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void getRowsWithWhereWithCompactMap() {
    RestApiV2QMapTestsImplIT.getRowsWithWhereWithCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.getRowsWithWhereWithCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void getRowsWithWhereWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.getRowsWithWhereWithNonCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.getRowsWithWhereWithNonCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Test
  @Override
  public void getAllIndexesWithCompactMap() {
    RestApiV2QMapTestsImplIT.getAllIndexesWithCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.getAllIndexesWithCompactMap(this, COMPACT_MAP_DATA, false);
  }

  @Override
  @Test
  public void getAllIndexesWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.getAllIndexesWithNonCompactMap(this, COMPACT_MAP_DATA, true);
    RestApiV2QMapTestsImplIT.getAllIndexesWithNonCompactMap(this, COMPACT_MAP_DATA, false);
  }
}
