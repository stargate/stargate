package io.stargate.web.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SuccessResponse {
  final boolean success = true;

  public boolean getSuccess() {
    return success;
  }
}