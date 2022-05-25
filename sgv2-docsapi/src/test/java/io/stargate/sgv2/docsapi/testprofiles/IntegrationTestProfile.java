package io.stargate.sgv2.docsapi.testprofiles;

import io.quarkus.test.junit.QuarkusTestProfile;
import io.stargate.sgv2.docsapi.testresource.StargateTestResource;
import java.util.Collections;
import java.util.List;

/** Test profile for integration tests. Includes the {@link StargateTestResource}. */
public class IntegrationTestProfile implements QuarkusTestProfile {

  /** Adds StargateTestResource to the test resources. */
  @Override
  public List<TestResourceEntry> testResources() {
    TestResourceEntry stargateResource = new TestResourceEntry(StargateTestResource.class);
    return Collections.singletonList(stargateResource);
  }
}
