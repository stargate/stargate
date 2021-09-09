package io.stargate.web.docsapi.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum BuiltInApiFunction {
  ARRAY_PUSH("$push", "Appends data to the end of an array"),
  ARRAY_POP("$pop", "Removes data from the end of an array, returning it");

  @JsonProperty("name")
  public String name;

  @JsonProperty("description")
  public String description;

  public boolean requiresValue() {
    return this == ARRAY_PUSH;
  }

  BuiltInApiFunction(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public static BuiltInApiFunction fromName(String name) {
    for (BuiltInApiFunction apiFunc : BuiltInApiFunction.values()) {
      if (apiFunc.name.equals(name)) {
        return apiFunc;
      }
    }
    throw new IllegalArgumentException("No BuiltInApiFunction found for name: " + name);
  }
}
