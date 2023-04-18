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
package io.stargate.sgv2.docsapi.api.v2.schemas.namespaces.model.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
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
