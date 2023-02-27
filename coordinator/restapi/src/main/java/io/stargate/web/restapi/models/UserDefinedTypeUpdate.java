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

import com.datastax.oss.driver.shaded.guava.common.base.MoreObjects;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import javax.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserDefinedTypeUpdate {

  @NotNull private String name;

  @JsonProperty("addFields")
  private List<UserDefinedTypeField> addFields;

  @JsonProperty("renameFields")
  private List<RenameUdtField> renameFields;

  @ApiModelProperty(required = true, value = "User Defined Type name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @ApiModelProperty(value = "User Defined Type fields to add")
  public List<UserDefinedTypeField> getAddFields() {
    return addFields;
  }

  public void setAddFields(List<UserDefinedTypeField> addFields) {
    this.addFields = addFields;
  }

  @ApiModelProperty(value = "User Defined Type fields to rename")
  public List<RenameUdtField> getRenameFields() {
    return renameFields;
  }

  public void setRenameFields(List<RenameUdtField> renameFields) {
    this.renameFields = renameFields;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("addFields", addFields)
        .add("renameFields", renameFields)
        .omitNullValues()
        .toString();
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class RenameUdtField {

    private String from;
    private String to;

    @ApiModelProperty(value = "User Defined Type's old field name")
    public String getFrom() {
      return from;
    }

    public void setFrom(String from) {
      this.from = from;
    }

    @ApiModelProperty(value = "User Defined Type's new field name")
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
