package io.stargate.sgv2.restsvc.models;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;

@ApiModel(
    description =
        "Defines a column list for the primary key. Can be either a single column, compound primary key, or composite partition key. Provide multiple columns for the partition key to define a composite partition key.")
public class Sgv2PrimaryKey {
  private List<String> partitionKey;
  private List<String> clusteringKey;

  public Sgv2PrimaryKey(final List<String> partitionKey, final List<String> clusteringKey) {
    this.partitionKey = partitionKey;
    this.clusteringKey = clusteringKey;
  }

  @ApiModelProperty(
      required = true,
      value = "Name of the column(s) that constitute the partition key.")
  public List<String> getPartitionKey() {
    return partitionKey;
  }

  @ApiModelProperty(value = "Name of the column or columns that constitute the clustering key.")
  public List<String> getClusteringKey() {
    return clusteringKey;
  }

  public void setPartitionKey(List<String> partitionKey) {
    this.partitionKey = partitionKey;
  }

  public void setClusteringKey(List<String> clusteringKey) {
    this.clusteringKey = clusteringKey;
  }
}
