package io.stargate.web.models;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RowResponse {
  int count;
  List<Map<String, Object>> rows;

  public int getCount() {
    return count;
  }

  public List<Map<String, Object>> getRows() {
    return rows;
  }

  @JsonCreator
  public RowResponse(@JsonProperty("count") int count, @JsonProperty("rows") List<Map<String, Object>> rows) {
    this.count = count;
    this.rows = rows;
  }
}