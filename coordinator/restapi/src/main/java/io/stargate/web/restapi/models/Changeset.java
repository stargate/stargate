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
package io.stargate.web.restapi.models;

import io.swagger.annotations.ApiModelProperty;

public class Changeset {

  private String column;
  private String value;

  @ApiModelProperty(example = "firstName", required = true, value = "Name of the column to update.")
  public String getColumn() {
    return column;
  }

  public Changeset setColumn(String column) {
    this.column = column;
    return this;
  }

  @ApiModelProperty(
      example = "Joe",
      required = true,
      value = "The value to update in the column for all matching rows.")
  public String getValue() {
    return value;
  }

  public Changeset setValue(String value) {
    this.value = value;
    return this;
  }
}
