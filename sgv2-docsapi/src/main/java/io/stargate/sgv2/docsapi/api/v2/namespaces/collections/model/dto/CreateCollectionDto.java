package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.model.dto;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

public record CreateCollectionDto(
    @Schema(description = "The name of the collection.", pattern = "\\w+", example = "cycling")
        @NotNull(message = "`name` is required to create a collection")
        @NotBlank(message = "`name` is required to create a collection")
        String name) {}
