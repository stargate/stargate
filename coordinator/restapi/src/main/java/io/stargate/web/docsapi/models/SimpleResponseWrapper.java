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
package io.stargate.web.docsapi.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

/**
 * Response wrapper used for those Document API endpoints that do not need information provided by
 * {@link DocumentResponseWrapper}.
 *
 * @param <T> Type of response wrapped
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SimpleResponseWrapper<T> {
  @JsonProperty("data")
  private T data;

  @ApiModelProperty(value = "Response data returned by the request.")
  public T getData() {
    return data;
  }

  public SimpleResponseWrapper setData(T data) {
    this.data = data;
    return this;
  }

  @JsonCreator
  public SimpleResponseWrapper(@JsonProperty("data") final T data) {
    this.data = data;
  }
}
