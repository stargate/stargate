package io.stargate.sgv2.it;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QMapCompactIT extends RestApiV2QIntegrationTestBase
    implements RestApiV2QMapTests {
  private static final boolean SERVER_FLAG = true;

  public RestApiV2QMapCompactIT() {
    super("maptest_ks_", "maptest_t_", KeyspaceCreation.PER_CLASS);
  }

  @Test
  @Override
  public void addRowWithCompactMap() {
    RestApiV2QMapTestsImplIT.addRowWithCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.addRowWithCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void addRowWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.addRowWithNonCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.addRowWithNonCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void updateRowWithCompactMap() {
    RestApiV2QMapTestsImplIT.updateRowWithCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.updateRowWithCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void updateRowWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.updateRowWithNonCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.updateRowWithNonCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void patchRowWithCompactMap() {
    RestApiV2QMapTestsImplIT.patchRowWithCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.patchRowWithCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void patchRowWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.patchRowWithNonCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.patchRowWithNonCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void getRowsWithCompactMap() {
    RestApiV2QMapTestsImplIT.getRowsWithCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.getRowsWithCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void getRowsWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.getRowsWithNonCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.getRowsWithNonCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void getAllRowsWithCompactMap() {
    RestApiV2QMapTestsImplIT.getAllRowsWithCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.getAllRowsWithCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void getAllRowsWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.getAllRowsWithNonCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.getAllRowsWithNonCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void getRowsWithWhereWithCompactMap() {
    RestApiV2QMapTestsImplIT.getRowsWithWhereWithCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.getRowsWithWhereWithCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void getRowsWithWhereWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.getRowsWithWhereWithNonCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.getRowsWithWhereWithNonCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void getAllIndexesWithCompactMap() {
    RestApiV2QMapTestsImplIT.getAllIndexesWithCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.getAllIndexesWithCompactMap(this, SERVER_FLAG, false);
  }

  @Override
  @Test
  public void getAllIndexesWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.getAllIndexesWithNonCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.getAllIndexesWithNonCompactMap(this, SERVER_FLAG, false);
  }
}
