package io.stargate.web.models;

import java.util.ArrayList;
import java.util.List;

public class PrimaryKey {
  List<String> partitionKey = new ArrayList<>();
  List<String> clusteringKey = new ArrayList<>();

  public PrimaryKey() {}

  public PrimaryKey(final List<String> partitionKey, final List<String> clusteringKey) {
    this.partitionKey = partitionKey;
    this.clusteringKey = clusteringKey;
  }

  public List<String> getPartitionKey() {
    return partitionKey;
  }

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