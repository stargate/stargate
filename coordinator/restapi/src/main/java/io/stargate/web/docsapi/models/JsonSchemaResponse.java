package io.stargate.web.docsapi.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.core.report.LogLevel;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonSchemaResponse {
  private final JsonNode schema;

  private List<String> debug;
  private List<String> info;
  private List<String> warn;
  private List<String> error;
  private List<String> fatal;

  @JsonProperty("schema")
  public JsonNode getSchema() {
    return schema;
  }

  @JsonProperty("debug")
  public List<String> getDebug() {
    return debug;
  }

  @JsonProperty("info")
  public List<String> getInfo() {
    return info;
  }

  @JsonProperty("warn")
  public List<String> getWarn() {
    return warn;
  }

  @JsonProperty("error")
  public List<String> getError() {
    return error;
  }

  @JsonProperty("fatal")
  public List<String> getFatal() {
    return fatal;
  }

  @JsonCreator
  public JsonSchemaResponse(@JsonProperty("schema") JsonNode schema) {
    this.schema = schema;
  }

  @Override
  public String toString() {
    return String.format(
        "JsonSchemaResponse(schema=%s, debug=%s, info=%s, warn=%s, error=%s, fatal=%s)",
        schema, debug, info, warn, error, fatal);
  }

  public void addMessage(LogLevel level, String message) {
    if (level == LogLevel.DEBUG) {
      if (debug == null) {
        debug = new ArrayList<>();
      }
      debug.add(message);
    } else if (level == LogLevel.INFO) {
      if (info == null) {
        info = new ArrayList<>();
      }
      info.add(message);
    } else if (level == LogLevel.WARNING) {
      if (warn == null) {
        warn = new ArrayList<>();
      }
      warn.add(message);
    } else if (level == LogLevel.ERROR) {
      if (error == null) {
        error = new ArrayList<>();
      }
      error.add(message);
    } else if (level == LogLevel.FATAL) {
      if (fatal == null) {
        fatal = new ArrayList<>();
      }
      fatal.add(message);
    }
  }
}
