package io.stargate.sgv2.docsapi.api.v2.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.stargate.sgv2.docsapi.models.ExecutionProfile;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record MultiDocsResponse(
    @Schema(
            description = "The ids of the documents.",
            example =
                "[822dc277-9121-4791-8b01-da8154e67d5d, 6334dft4-9153-3642-4f32-da8154e67d5d]")
        List<String> documentIds,

    // execution profile
    ExecutionProfile profile) {}
