package io.stargate.it.http.docsapi;

import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;

@StargateSpec(parametersCustomizer = "disableLoggedBatches")
public class DocumentApiV2TestUnloggedBatches extends BaseDocumentApiV2Test {
    @SuppressWarnings("unused") // referenced in @StargateSpec
    public static void disableLoggedBatches(StargateParameters.Builder builder) {
        builder.putSystemProperties("stargate.persistence.loggedbatches.support.default", "false");
    }
}
