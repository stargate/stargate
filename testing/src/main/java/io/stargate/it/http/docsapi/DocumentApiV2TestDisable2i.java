package io.stargate.it.http.docsapi;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.stargate.it.storage.ClusterConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;

@StargateSpec(parametersCustomizer = "disable2i")
@Order(Integer.MAX_VALUE)
public class DocumentApiV2TestDisable2i extends BaseDocumentApiV2Test {
  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void disable2i(StargateParameters.Builder builder) {
    builder.putSystemProperties("stargate.persistence.2i.support.default", "false");
  }

  @BeforeAll
  public static void checkBackendCapabilities(ClusterConnectionInfo backend) {
    // Docs API requires either SAI or 2i to be supported therefore this test is not meaningful
    // on backends that do not support SAI.
    assumeTrue(backend.supportsSAI());
  }
}
