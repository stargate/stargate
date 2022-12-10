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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.restapi.service.models;

import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@RegisterForReflection
@Schema(name = "RowsResponse")
public record Sgv2RowsResponse(
    @Schema(description = "The count of records returned.") int count,
    @Schema(
            description =
                "A string representing the paging state to be used on future paging requests.")
        String pageState,
    @Schema(description = "The rows returned by the request.") List<Map<String, Object>> data) {}
