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
    @Schema(
            description =
                "Profiling information related to the execution of the request. Only set if the endpoint supports profiling and `profile` query parameter is set to `true`.",
            nullable = true,
            example =
                """
                {
                   "description":"root",
                   "queries":[],
                   "nested":[
                      {
                         "description":"FILTER: speed LT 1000",
                         "queries":[
                            {
                               "cql":"SELECT key, leaf, WRITETIME(leaf) FROM cycling.events WHERE p0 = ? AND leaf = ? AND p1 = ? AND dbl_value < ? ALLOW FILTERING",
                               "executionCount":1,
                               "rowCount":1
                            }
                         ],
                         "nested":[]
                      },
                      {
                         "description":"LoadProperties",
                         "queries":[
                            {
                               "cql":"SELECT key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM cycling.events WHERE key = ?",
                               "executionCount":1,
                               "rowCount":21
                            }
                         ],
                         "nested":[]
                      }
                   ]
                }
                """)
        ExecutionProfile profile) {}
