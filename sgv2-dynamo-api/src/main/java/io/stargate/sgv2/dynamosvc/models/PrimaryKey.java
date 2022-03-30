package io.stargate.sgv2.dynamosvc.models;

public class PrimaryKey {
  private String partitionKey;
  private String clusteringKey;

  public String getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = partitionKey;
  }

  public String getClusteringKey() {
    return clusteringKey;
  }

  public void setClusteringKey(String clusteringKey) {
    this.clusteringKey = clusteringKey;
  }
}
