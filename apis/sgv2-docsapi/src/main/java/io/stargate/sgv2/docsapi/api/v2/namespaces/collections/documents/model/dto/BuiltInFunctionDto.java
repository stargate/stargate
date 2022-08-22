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

package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.documents.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** DTO for a namespace. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BuiltInFunctionDto(
    @Schema(
            description = "The operation to execute",
            pattern = "\\$pop|\\$push|\\$set",
            example = "$push")
        @NotNull(message = "`operation` is required for function execution")
        @NotBlank(message = "`operation` is required for function execution")
        @Pattern(
            regexp = "\\$pop|\\$push|\\$set",
            message = "available built-in functions are [$pop, $push, $set]")
        String operation,
    @Schema(
            description = "The value to use for the operation",
            example = "some_value",
            nullable = true)
        JsonNode value) {}
