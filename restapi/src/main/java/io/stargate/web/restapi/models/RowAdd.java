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
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;

public class RowAdd {
  @NotEmpty(message = "Columns cannot be null or empty")
  @Valid
  private List<ColumnModel> columns;

  @ApiModelProperty(required = true, value = "The column definitions belonging to the row to add.")
  public List<ColumnModel> getColumns() {
    return columns;
  }

  public void setColumns(List<ColumnModel> columns) {
    this.columns = columns;
  }

  public RowAdd() {}

  public RowAdd(List<ColumnModel> columns) {
    this.columns = columns;
  }
}
