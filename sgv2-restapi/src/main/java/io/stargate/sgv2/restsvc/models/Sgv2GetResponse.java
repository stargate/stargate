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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sgv2GetResponse<T> {
  @JsonProperty("count")
  protected final int count;

  @JsonProperty("pageState")
  protected final String pageState;

  @JsonProperty("data")
  protected T data;

  @ApiModelProperty(value = "The count of records returned")
  public int getCount() {
    return count;
  }

  @ApiModelProperty(
      value = "A string representing the paging state to be used on future paging requests.")
  public String getPageState() {
    return pageState;
  }

  @ApiModelProperty(value = "The data returned by the request")
  public T getData() {
    return data;
  }

  public Sgv2GetResponse setData(T data) {
    this.data = data;
    return this;
  }

  @JsonCreator
  public Sgv2GetResponse(
      @JsonProperty("count") final int count,
      @JsonProperty("pageState") final String pageState,
      @JsonProperty("data") final T data) {
    this.count = count;
    this.pageState = pageState;
    this.data = data;
  }
}
