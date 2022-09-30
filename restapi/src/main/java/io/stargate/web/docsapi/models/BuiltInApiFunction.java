package io.stargate.web.docsapi.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum BuiltInApiFunction {
  ARRAY_PUSH("$push", "Appends data to the end of an array"),
  ARRAY_POP("$pop", "Removes data from the end of an array, returning it");

  public String name;

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
    throw new ErrorCodeRuntimeException(
        ErrorCode.DOCS_API_INVALID_BUILTIN_FUNCTION,
        "No BuiltInApiFunction found for name: " + name);
  }
}
