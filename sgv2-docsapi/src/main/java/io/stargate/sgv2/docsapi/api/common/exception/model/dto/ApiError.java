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

import lombok.Builder;
import lombok.Value;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Value
@Builder
public class ApiError {

    @Schema(description = "A human readable description of the error state.", example = "//TODO")
    String description;

    @Schema(description = "The internal number referencing the error state.", example = "//TODO")
    int code;

    @Schema(hidden = true)
    String internalTxId;

}
