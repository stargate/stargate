package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import javax.validation.constraints.NotNull;

@ApiModel(value = "UserDefinedTypeAdd")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sgv2UDTAddRequest {
  @NotNull private String name;

  @JsonProperty("fields")
  @NotNull
  private List<Sgv2UDT.UDTField> fields;

  private boolean ifNotExists;

  @ApiModelProperty(required = true, value = "User Defined Type name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @ApiModelProperty(
      value =
          "Determines whether to create a new UDT if an UDT with the same name exists. Attempting to create an existing UDT returns an error unless this option is true.")
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @ApiModelProperty(required = true, value = "User Defined Type fields")
  public List<Sgv2UDT.UDTField> getFields() {
    return fields;
  }

  public void setFields(List<Sgv2UDT.UDTField> fields) {
    this.fields = fields;
  }
}
