package io.stargate.sgv2.it;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.junit.jupiter.api.Test;

import java.util.Map;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestProfile(RestApiV2QMapOptimizedDisabledIT.Profile.class)
public class RestApiV2QMapOptimizedDisabledIT extends RestApiV2QIntegrationTestBase
    implements RestApiV2QMapTests {
  private static final boolean SERVER_FLAG = false;

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("stargate.rest.optimize-map-data", Boolean.toString(SERVER_FLAG));
    }
  }

  public RestApiV2QMapOptimizedDisabledIT() {
    super("rowadd_ks_", "rowadd_t_", KeyspaceCreation.PER_CLASS);
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
  public void deleteRowWithCompactMap() {
    RestApiV2QMapTestsImplIT.deleteRowWithCompactMap(this, SERVER_FLAG, true);
    RestApiV2QMapTestsImplIT.deleteRowWithCompactMap(this, SERVER_FLAG, false);
  }

  @Test
  @Override
  public void deleteRowWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.deleteRowWithNonCompactMap(this);
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
    RestApiV2QMapTestsImplIT.getAllIndexesWithCompactMap(this);
  }

  @Override
  @Test
  public void getAllIndexesWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.getAllIndexesWithNonCompactMap(this);
  }

  @Test
  @Override
  public void findAllTypesWithCompactMap() {
    RestApiV2QMapTestsImplIT.findAllTypesWithCompactMap(this);
  }

  @Test
  @Override
  public void findAllTypesWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.findAllTypesWithNonCompactMap(this);
  }

  @Test
  @Override
  public void findTypeByIdWithCompactMap() {
    RestApiV2QMapTestsImplIT.findTypeByIdWithCompactMap(this);
  }

  @Test
  @Override
  public void findTypeByIdWithNonCompactMap() {
    RestApiV2QMapTestsImplIT.findTypeByIdWithNonCompactMap(this);
  }
}
