package io.stargate.sgv2.restapi.service.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import javax.validation.constraints.NotBlank;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

// Copied from SGv1 ColumnDefinition
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(name = "ColumnDefinition")
@RegisterForReflection
public record Sgv2ColumnDefinition(
    @Schema(
            example = "emailaddress",
            required = true,
            description = "Name for the column, which must be unique.")
        @NotBlank(message = "columnDefinition.name must be provided")
        String name,
    @Schema(
            example = "text",
            required = true,
            description = "The type of data allowed in the column.")
        String typeDefinition,
    @Schema(description = "Denotes whether the column is shared by all rows of a partition.",
            defaultValue="false")
        @JsonProperty("static")
        boolean isStatic) {
  // NOTE: unfortunately we need extra annotations here for renaming: this is a bug in Jackson
  // that hopefully gets resolved in future but until then property renaming along with custom
  // constructor needs redundant annotations
  @JsonCreator
  public Sgv2ColumnDefinition(
      @JsonProperty("name") String name,
      @JsonProperty("typeDefinition") final String typeDefinition,
      @JsonProperty("static") final boolean isStatic) {
    this.name = (name == null) ? "" : name;
    this.typeDefinition = (typeDefinition == null) ? "" : typeDefinition;
    this.isStatic = isStatic;
  }
}
