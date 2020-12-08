package io.stargate.it.http.docsapi;

import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;

@StargateSpec(parametersCustomizer = "disable2i")
public class DocumentApiV2TestDisable2i extends BaseDocumentApiV2Test {
  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void disable2i(StargateParameters.Builder builder) {
    builder.putSystemProperties("stargate.persistence.2i.support.default", "false");
  }
}
