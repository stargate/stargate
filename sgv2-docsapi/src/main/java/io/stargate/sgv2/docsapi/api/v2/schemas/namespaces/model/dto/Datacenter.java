package io.stargate.sgv2.docsapi.api.v2.schemas.namespaces.model.dto;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** Datacenter of a namespace. */
public record Datacenter(

    // name
    @Schema(description = "The name of the datacenter.", example = "dc1")
        @NotNull(message = "a datacenter `name` is required when using `NetworkTopologyStrategy`")
        @NotBlank(message = "a datacenter `name` is required when using `NetworkTopologyStrategy`")
        String name,

    // replicas
    @Schema(
            description =
                "The number of replicas in the datacenter. In other words, the number of copies of each row in the datacenter.",
            defaultValue = "3",
            example = "3")
        @Min(value = 1, message = "minimum amount of `replicas` for a datacenter is one")
        Integer replicas) {}
