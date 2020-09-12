package io.stargate.web.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClusteringExpression {
  final String column;
  final String order;

  @JsonCreator
  public ClusteringExpression(
          @JsonProperty("name") String column,
          @JsonProperty("order") String order) {
    this.column = column;
    this.order = order;
  }

  public String getColumn() {
    return column;
  }

  public String getOrder() {
    return order;
  }
}