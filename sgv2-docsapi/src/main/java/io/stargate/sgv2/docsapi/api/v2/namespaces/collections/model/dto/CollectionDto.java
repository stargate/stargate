package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * DTO for the get collections response.
 *
 * @param name Name of the collection.
 * @param upgradeAvailable If upgrade is available.
 * @param upgradeType Type of upgrade, if available.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CollectionDto(
    @Schema(description = "The name of the collection.", pattern = "\\w+", example = "cycling")
        String name,
    @Schema(
            description =
                "Whether an upgrade is available. Use the 'upgrade a collection' endpoint with the upgrade type to perform the upgrade.",
            example = "false")
        boolean upgradeAvailable,
    @Schema(
            description = "The upgrade type, if an upgrade is available.",
            nullable = true,
            implementation = CollectionUpgradeType.class,
            example = "null")
        CollectionUpgradeType upgradeType) {}
