package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.model.dto;

import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Request body when upgrading an existing collection.
 *
 * @param upgradeType Type of the upgrade.
 */
public record UpgradeCollectionDto(
    @Schema(
            description = "The upgrade type to perform.",
            implementation = CollectionUpgradeType.class)
        @NotNull(message = "`upgradeType` is required to upgrade a collection")
        CollectionUpgradeType upgradeType) {}
