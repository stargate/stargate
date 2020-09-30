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
package io.stargate.web.models;

import io.swagger.annotations.ApiModelProperty;
import java.util.List;

public class Filter {
  public enum Operator {
    eq,
    notEq,
    gt,
    gte,
    lt,
    lte,
    in,
  }

  String columnName;
  Operator operator;
  List<Object> value;

  @ApiModelProperty(value = "The column name to apply the filter to.")
  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  @ApiModelProperty(value = "")
  public Operator getOperator() {
    return operator;
  }

  public void setOperator(Operator operator) {
    this.operator = operator;
  }

  @ApiModelProperty(
      value =
          "An array of values to use in the filter. The full array will only be used for the `in` operation, for all others only the first element will be considered.")
  public List<Object> getValue() {
    return value;
  }

  public void setValue(List<Object> value) {
    this.value = value;
  }
}
