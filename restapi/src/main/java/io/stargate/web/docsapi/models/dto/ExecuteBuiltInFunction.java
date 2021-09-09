package io.stargate.web.docsapi.models.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.BuiltInApiFunction;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/** The DTO for the execution of a built-in function. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExecuteBuiltInFunction {

  @JsonProperty("operation")
  @NotNull(message = "a valid `operation` is required")
  @NotBlank(message = "a valid `operation` is required")
  private final String operation;

  private final BuiltInApiFunction function;
  private final Object value;

  @JsonCreator
  public ExecuteBuiltInFunction(
      @JsonProperty("operation") String operation, @JsonProperty("value") Object value) {
    this.operation = operation;
    this.function = BuiltInApiFunction.fromName(operation);
    if (this.function.requiresValue() && value == null) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Provided value must not be null");
    }
    this.value = value;
  }

  @ApiModelProperty(required = true, value = "The name of the operation.", example = "$push")
  @JsonProperty("operation")
  public String getOperation() {
    return operation;
  }

  public BuiltInApiFunction getFunction() {
    return function;
  }

  @ApiModelProperty(value = "The value to use for the operation", example = "some_value")
  @JsonProperty("value")
  public Object getValue() {
    return value;
  }
}
