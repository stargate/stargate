package io.stargate.sgv2.docsapi.testprofiles;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

/**
 * Simple test profile to enable SAI on datastore.
 *
 * <p>Annotate test class with @TestProfile(SaiEnabledTestProfile.class) to use.
 */
public class SaiEnabledTestProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    // adapt consistency, depth and column name(s)
    return ImmutableMap.<String, String>builder()
        .put("stargate.data-store.sai-enabled", "true")
        .build();
  }
}
