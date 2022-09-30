/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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
