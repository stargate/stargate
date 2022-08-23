package io.stargate.sgv2.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QSchemaColumnsIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QSchemaColumnsIT() {
    super("col_ks_", "col_t_");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Add (Create)
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnAddBadType() {}

  @Test
  public void columnAddBasic() {}

  @Test
  public void columnAddStatic() {}

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Get single
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnGetBadKeyspace() {}

  @Test
  public void columnGetBadTable() {}

  @Test
  public void columnGetComplex() {}

  @Test
  public void columnGetNotFound() {}

  @Test
  public void columnGetRaw() {}

  @Test
  public void columnGetWrapped() {}

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Get multiple
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnsGetBadKeyspace() {}

  @Test
  public void columnsGetBadTable() {}

  @Test
  public void columnsGetComplex() {}

  @Test
  public void columnsGetRaw() {}

  @Test
  public void columnsGetWrapped() {}

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Update
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnUpdate() {}

  @Test
  public void columnUpdateBadKeyspace() {}

  @Test
  public void columnUpdateBadTable() {}

  @Test
  public void columnUpdateNotFound() {}

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Delete
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnDelete() {}

  @Test
  public void columnDeleteBadKeyspace() {}

  @Test
  public void columnDeleteBadTable() {}

  @Test
  public void columnDeleteNotFound() {}

  @Test
  public void columnDeletePartitionKey() {}
}
