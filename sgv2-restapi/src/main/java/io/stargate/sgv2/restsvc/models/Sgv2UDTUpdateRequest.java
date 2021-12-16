package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import javax.validation.constraints.NotNull;

@ApiModel(value = "UserDefinedTypeUpdate")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sgv2UDTUpdateRequest {
  @NotNull private String name;

  @JsonProperty("addFields")
  private List<Sgv2UDT.UDTField> addFields;

  @JsonProperty("renameFields")
  private List<FieldRename> renameFields;

  @ApiModelProperty(required = true, value = "User Defined Type name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @ApiModelProperty(value = "User Defined Type fields to add")
  public List<Sgv2UDT.UDTField> getAddFields() {
    return addFields;
  }

  public void setAddFields(List<Sgv2UDT.UDTField> addFields) {
    this.addFields = addFields;
  }

  @ApiModelProperty(value = "User Defined Type fields to rename")
  public List<FieldRename> getRenameFields() {
    return renameFields;
  }

  public void setRenameFields(List<FieldRename> renameFields) {
    this.renameFields = renameFields;
  }

  @ApiModel(value = "UserDefinedTypeFieldRename")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class FieldRename {

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
  }
}
