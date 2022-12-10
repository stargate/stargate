package io.stargate.sgv2.restapi.service.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(name = "UserDefinedType")
@JsonInclude(JsonInclude.Include.NON_NULL)
@RegisterForReflection
public record Sgv2UDT(
    @Schema(description = "The name of the user defined type.") String name,
    @Schema(description = "Name of the keyspace the user defined type belongs.") String keyspace,
    @Schema(description = "Definition of columns within the user defined type.")
        List<UDTField> fields) {
  /** Represents a column in a User Defined type ({@link Sgv2UDT}) */
  @Schema(name = "UserDefinedTypeField")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record UDTField(
      @Schema(
              example = "emailaddress",
              required = true,
              description = "Name for the type, which must be unique.")
          String name,
      @Schema(
              example = "text",
              required = true,
              description = "A valid type of data (e.g, text, int, etc) allowed in the type.")
          String typeDefinition) {}
}
