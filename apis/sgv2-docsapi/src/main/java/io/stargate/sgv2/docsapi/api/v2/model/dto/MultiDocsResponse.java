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
