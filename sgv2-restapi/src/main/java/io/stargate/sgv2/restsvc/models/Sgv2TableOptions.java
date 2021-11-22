package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;

// copy of SGv1 TableOptions
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sgv2TableOptions {
  private Integer defaultTimeToLive;
  private List<Sgv2ClusteringExpression> clusteringExpression;

  @JsonCreator
  public Sgv2TableOptions(
      @JsonProperty("defaultTimeToLive") final int defaultTimeToLive,
      @JsonProperty("clusteringExpression")
          final List<Sgv2ClusteringExpression> clusteringExpression) {
    this.defaultTimeToLive = defaultTimeToLive;
    this.clusteringExpression = clusteringExpression;
  }

  @ApiModelProperty(
      value =
          "Defines the Time To Live (TTL), which determines the time period (in seconds) to expire data. If the value is >0, TTL is enabled for the entire table and an expiration timestamp is added to each column. The maximum value is 630720000 (20 years). A new TTL timestamp is calculated each time the data is updated and the row is removed after the data expires.")
  public Integer getDefaultTimeToLive() {
    return defaultTimeToLive;
  }

  @ApiModelProperty(
      value =
          "Order rows storage to make use of the on-disk sorting of columns. Specifying order can make query results more efficient. Defaults to ascending if not provided.")
  public List<Sgv2ClusteringExpression> getClusteringExpression() {
    return clusteringExpression;
  }

  public void setDefaultTimeToLive(int defaultTimeToLive) {
    this.defaultTimeToLive = defaultTimeToLive;
  }

  public void setClusteringExpression(List<Sgv2ClusteringExpression> clusteringExpression) {
    this.clusteringExpression = clusteringExpression;
  }
}
