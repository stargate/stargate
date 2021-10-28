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
package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import java.util.Map;

public class Sgv2Rows extends Sgv2GetResponse<List<Map<String, Object>>> {
  // Override to add better description
  @ApiModelProperty(value = "The rows returned by the request.")
  public List<Map<String, Object>> getData() {
    return data;
  }

  @JsonCreator
  public Sgv2Rows(
      @JsonProperty("count") int count,
      @JsonProperty("pageState") String pageState,
      @JsonProperty("rows") List<Map<String, Object>> rows) {
    super(count, pageState, rows);
  }
}
