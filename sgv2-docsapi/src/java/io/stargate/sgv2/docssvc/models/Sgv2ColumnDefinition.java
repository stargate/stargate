package java.io.stargate.sgv2.docssvc.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// Copied from SGv1 ColumnDefinition
@JsonInclude(JsonInclude.Include.NON_NULL)
@ApiModel(value = "ColumnDefinition")
public class Sgv2ColumnDefinition {
  private String name;
  private String typeDefinition;
  private boolean isStatic;

  @JsonCreator
  public Sgv2ColumnDefinition(
      @JsonProperty("name") final String name,
      @JsonProperty("typeDefinition") final String typeDefinition,
      @JsonProperty("static") final boolean isStatic) {
    this.name = (name == null) ? "" : name;
    this.typeDefinition = (typeDefinition == null) ? "" : typeDefinition;
    this.isStatic = isStatic;
  }

  @ApiModelProperty(
      example = "emailaddress",
      required = true,
      value = "Name for the column, which must be unique.")
  public String getName() {
    return name;
  }

  @ApiModelProperty(
      example = "text",
      required = true,
      value = "The type of data allowed in the column.")
  public String getTypeDefinition() {
    return typeDefinition;
  }

  @ApiModelProperty(value = "Denotes whether the column is shared by all rows of a partition.")
  @JsonProperty("static")
  public boolean getIsStatic() {
    return isStatic;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setTypeDefinition(String typeDefinition) {
    this.typeDefinition = typeDefinition;
  }

  public void setIsStatic(boolean isStatic) {
    this.isStatic = isStatic;
  }
}
