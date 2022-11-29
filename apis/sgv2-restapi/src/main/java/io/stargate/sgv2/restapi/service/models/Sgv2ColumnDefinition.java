package io.stargate.sgv2.restapi.service.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.Objects;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

// Copied from SGv1 ColumnDefinition
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(name = "ColumnDefinition")
@RegisterForReflection
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

  @Schema(
      example = "emailaddress",
      required = true,
      description = "Name for the column, which must be unique.")
  public String getName() {
    return name;
  }

  @Schema(
      example = "text",
      required = true,
      description = "The type of data allowed in the column.")
  public String getTypeDefinition() {
    return typeDefinition;
  }

  @Schema(description = "Denotes whether the column is shared by all rows of a partition.")
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

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Sgv2ColumnDefinition)) return false;
    Sgv2ColumnDefinition other = (Sgv2ColumnDefinition) o;
    return (this.isStatic == other.isStatic)
        && Objects.equals(this.name, other.name)
        && Objects.equals(this.typeDefinition, other.typeDefinition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isStatic, name, typeDefinition);
  }

  @Override
  public String toString() {
    return new StringBuilder(60)
        .append("[")
        .append(getClass().getSimpleName())
        .append(":name=")
        .append(name)
        .append(",typeDefinition=")
        .append(typeDefinition)
        .append(",isStatic=")
        .append(isStatic)
        .append("]")
        .toString();
  }
}
