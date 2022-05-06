package io.stargate.sgv2.docsapi.testprofiles;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

/**
 * Simple test profile to have a switch to numeric booleans.
 *
 * <p>Annotate test class with @TestProfile(NumericBooleansTestProfile.class) to use.
 */
public class NumericBooleansTestProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    // adapt consistency, depth and column name(s)
    return ImmutableMap.<String, String>builder()
        .put("stargate.data-store.secondary-indexes-enabled", "false")
        .build();
  }
}
