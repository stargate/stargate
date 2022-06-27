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
package io.stargate.sgv2.docsapi.models;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableExecutionProfile.class)
@JsonDeserialize(as = ImmutableExecutionProfile.class)
@JsonPropertyOrder({"description", "queries", "nested"})
@Value.Immutable(lazyhash = true)
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
public interface ExecutionProfile {

  @Schema(description = "Brief information about this execution step.")
  String description();

  @Schema(description = "A set of CQL queries performed under this execution step.")
  List<QueryInfo> queries();

  @Schema(description = "Nested execution steps.", nullable = true)
  List<ExecutionProfile> nested();
}
