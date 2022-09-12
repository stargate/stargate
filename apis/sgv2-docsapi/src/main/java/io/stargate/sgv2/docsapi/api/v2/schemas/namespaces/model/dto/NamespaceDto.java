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

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Collection;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** DTO for a namespace. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record NamespaceDto(
    @Schema(description = "The name of the namespace.", pattern = "\\w+", example = "cycling")
        @NotNull(message = "`name` is required to create a namespace")
        @NotBlank(message = "`name` is required to create a namespace")
        String name,
    @Schema(
            description =
                "The amount of replicas to use with the `SimpleStrategy`. Ignored if `datacenters` is set.",
            nullable = true,
            defaultValue = "1",
            example = "1")
        @Min(value = 1, message = "minimum amount of `replicas` for `SimpleStrategy` is one")
        Integer replicas,
    @Schema(
            description =
                "The datacenters within a namespace. Only applies for those namespaces created with `NetworkTopologyStrategy`.",
            nullable = true)
        @Valid
        Collection<Datacenter> datacenters) {}
