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
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Response wrapper used for most Document API endpoints, where {@code documentId} and {@code
 * pageState} are to be returned along with wrapped response.
 *
 * @param <T> Type of response wrapped
 * @see SimpleResponseWrapper
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DocumentResponseWrapper<T>(

    // doc id
    @Schema(
            description = "The id of the document.",
            example = "822dc277-9121-4791-8b01-da8154e67d5d")
        String documentId,

    // page state
    @Schema(
            description =
                "A string representing the paging state to be used on future paging requests. Can be missing in case page state is exhausted.",
            nullable = true,
            example = "c29tZS1leGFtcGxlLXN0YXRl")
        String pageState,

    // data
    @Schema(description = "The data returned by the request.") T data,

    // execution profile
    ExecutionProfile profile) {}
