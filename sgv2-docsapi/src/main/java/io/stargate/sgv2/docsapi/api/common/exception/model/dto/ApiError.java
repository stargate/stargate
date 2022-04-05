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

package io.stargate.sgv2.docsapi.api.common.exception.model.dto;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Record for representing the API error.
 *
 * @param description  A human-readable description of the error state.
 * @param code         The internal number referencing the error state.
 * @param internalTxId The internal tracking number of the request.
 */
public record ApiError(

        @Schema(description = "A human readable description of the error state.", example = "Could not create collection `custom-users`, it has invalid characters. Valid characters are alphanumeric and underscores.")
        String description,

        @Schema(description = "The internal number referencing the error state.", example = "22000")
        int code,

        // TODO Is this still needed and used? This was not part of the public API.
        @Schema(hidden = true)
        String internalTxId) {

    public ApiError(String description) {
        this(description, 0, null);
    }

}
