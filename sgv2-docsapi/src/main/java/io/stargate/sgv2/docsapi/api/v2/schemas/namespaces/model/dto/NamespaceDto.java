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
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
/** DTO for namespace datacenter. */
public record NamespaceDto(

    // TODO should we return replicas count in case of the simple strategy
    //  that's not included in V1, but could be added here as non-breaking change

    // name
    @Schema(description = "The name of the namespace.", pattern = "\\w+", example = "cycling")
        String name,

    // replicas
    @Schema(
            description =
                "The datacenters within a keyspace. Only applies for those keyspaces created with `NetworkTopologyStrategy`.",
            nullable = true)
        List<Datacenter> datacenters) {}
