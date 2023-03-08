package io.stargate.it.http.docsapi;

import io.stargate.it.TestOrder;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import org.junit.jupiter.api.Order;

@StargateSpec(parametersCustomizer = "enableAll")
@Order(TestOrder.LAST)
public class DocumentApiV2Test extends BaseDocumentApiV2Test {
  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void enableAll(StargateParameters.Builder builder) {
    builder.putSystemProperties("stargate.persistence.2i.support.default", "true");
    builder.putSystemProperties("stargate.document_use_logged_batches", "true");
  }
}
