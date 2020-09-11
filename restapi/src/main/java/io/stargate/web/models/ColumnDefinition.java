package io.stargate.web.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColumnDefinition {
  String name;
  String typeDefinition;
  boolean isStatic;

  public ColumnDefinition(final String name, final String typeDefinition) {
    this(name, typeDefinition, false);
  }

  @JsonCreator
  public ColumnDefinition(
          @JsonProperty("name") final String name,
          @JsonProperty("typeDefinition") final String typeDefinition,
          @JsonProperty("static") final boolean isStatic) {
    this.name = name;
    this.typeDefinition = typeDefinition;
    this.isStatic = isStatic;
  }

  public String getName() {
    return name;
  }

  public String getTypeDefinition() {
    return typeDefinition;
  }

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