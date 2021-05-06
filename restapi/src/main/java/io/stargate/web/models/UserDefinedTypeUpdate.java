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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import javax.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserDefinedTypeUpdate {

  @NotNull private String name;

  @JsonProperty("add-type")
  private List<UserDefinedTypeField> fieldDefinitions;

  @JsonProperty("rename-type")
  private List<RenameUdtField> renameColumns;

  @ApiModelProperty(required = true, value = "User Defined Type name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @ApiModelProperty(value = "User Defined Type fields to add")
  public List<UserDefinedTypeField> getFieldDefinitions() {
    return fieldDefinitions;
  }

  public void setFieldDefinitions(List<UserDefinedTypeField> fieldDefinitions) {
    this.fieldDefinitions = fieldDefinitions;
  }

  @ApiModelProperty(value = "User Defined Type columns to rename")
  public List<RenameUdtField> getRenameColumns() {
    return renameColumns;
  }

  public void setRenameColumns(List<RenameUdtField> renameColumns) {
    this.renameColumns = renameColumns;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("add-type", fieldDefinitions)
        .add("rename-type", renameColumns)
        .omitNullValues()
        .toString();
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class RenameUdtField {

    private String from;
    private String to;

    @ApiModelProperty(value = "User Defined Type old column name")
    public String getFrom() {
      return from;
    }

    public void setFrom(String from) {
      this.from = from;
    }

    @ApiModelProperty(value = "User Defined Type new column name")
    public String getTo() {
      return to;
    }

    public void setTo(String to) {
      this.to = to;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("from", from)
          .add("to", to)
          .omitNullValues()
          .toString();
    }
  }
}
