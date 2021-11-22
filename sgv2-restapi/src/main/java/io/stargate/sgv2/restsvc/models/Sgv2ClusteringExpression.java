package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

// copied from SGv1 ClusteringExpression
public class Sgv2ClusteringExpression {
  private final String column;
  private final String order;

  @JsonCreator
  public Sgv2ClusteringExpression(
      @JsonProperty("name") String column, @JsonProperty("order") String order) {
    this.column = column;
    this.order = order;
  }

  @ApiModelProperty(required = true, value = "The name of the column to order by")
  public String getColumn() {
    return column;
  }

  @ApiModelProperty(required = true, value = "The clustering order", allowableValues = "asc,desc")
  public String getOrder() {
    return order;
  }
}
