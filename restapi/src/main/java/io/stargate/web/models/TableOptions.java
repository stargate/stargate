package io.stargate.web.models;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableOptions {
  Integer defaultTimeToLive;
  List<ClusteringExpression> clusteringExpression = new ArrayList<>();

  @JsonCreator
  public TableOptions(
          @JsonProperty("defaultTimeToLive") final int defaultTimeToLive,
          @JsonProperty("clusteringExpression") final List<ClusteringExpression> clusteringExpression) {
    this.defaultTimeToLive = defaultTimeToLive;
    this.clusteringExpression = clusteringExpression;
  }

  public Integer getDefaultTimeToLive() {
    return defaultTimeToLive;
  }

  public List<ClusteringExpression> getClusteringExpression() {
    return clusteringExpression;
  }

  public void setDefaultTimeToLive(int defaultTimeToLive) {
    this.defaultTimeToLive = defaultTimeToLive;
  }

  public void setClusteringExpression(List<ClusteringExpression> clusteringExpression) {
    this.clusteringExpression = clusteringExpression;
  }

  public TableOptions() {
  }
}