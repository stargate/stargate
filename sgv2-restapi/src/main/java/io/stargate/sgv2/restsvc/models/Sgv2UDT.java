package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sgv2UDT {
  private final String name;
  private final String keyspace;
  private final List<UDTField> fields;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public Sgv2UDT(
      @JsonProperty("name") final String name,
      @JsonProperty("keyspace") final String keyspace,
      @JsonProperty("fields") final List<UDTField> fields) {
    this.name = name;
    this.keyspace = keyspace;
    this.fields = fields;
  }

  @ApiModelProperty(value = "The name of the user defined type.")
  public String getName() {
    return name;
  }

  @ApiModelProperty(value = "Name of the keyspace the user defined type belongs.")
  public String getKeyspace() {
    return keyspace;
  }

  @ApiModelProperty(value = "Definition of columns within the user defined type.")
  public List<UDTField> getFields() {
    return fields;
  }

  /** Represents a column in a User Defined type ({@link Sgv2UDT}) */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class UDTField {
    private final String name;
    private final String typeDefinition;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public UDTField(
        @JsonProperty("name") final String name,
        @JsonProperty("typeDefinition") final String typeDefinition) {
      this.name = name;
      this.typeDefinition = typeDefinition;
    }

    @ApiModelProperty(
        example = "emailaddress",
        required = true,
        value = "Name for the type, which must be unique.")
    public String getName() {
      return name;
    }

    @ApiModelProperty(
        example = "text",
        required = true,
        value = "A valid type of data (e.g, text, int, etc) allowed in the type.")
    public String getTypeDefinition() {
      return typeDefinition;
    }
  }
}
