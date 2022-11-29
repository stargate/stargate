package io.stargate.sgv2.restapi.service.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(name = "UserDefinedType")
@JsonInclude(JsonInclude.Include.NON_NULL)
@RegisterForReflection
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

  @Schema(description = "The name of the user defined type.")
  public String getName() {
    return name;
  }

  @Schema(description = "Name of the keyspace the user defined type belongs.")
  public String getKeyspace() {
    return keyspace;
  }

  @Schema(description = "Definition of columns within the user defined type.")
  public List<UDTField> getFields() {
    return fields;
  }

  /** Represents a column in a User Defined type ({@link Sgv2UDT}) */
  @Schema(name = "UserDefinedTypeField")
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

    @Schema(
        example = "emailaddress",
        required = true,
        description = "Name for the type, which must be unique.")
    public String getName() {
      return name;
    }

    @Schema(
        example = "text",
        required = true,
        description = "A valid type of data (e.g, text, int, etc) allowed in the type.")
    public String getTypeDefinition() {
      return typeDefinition;
    }
  }
}
