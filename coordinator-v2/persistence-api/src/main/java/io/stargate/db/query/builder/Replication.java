package io.stargate.db.query.builder;

import static java.lang.String.format;

import java.util.Map;

/**
 * Represents a keyspace replication string.
 *
 * <p>So something like "{ 'class': 'SimpleStrategy', 'replication_factor': 1}".
 */
public class Replication {
  private final String replication;

  private Replication(String replication) {
    this.replication = replication;
  }

  public static Replication simpleStrategy(int replicationFactor) {
    return new Replication(
        format("{ 'class': 'SimpleStrategy', 'replication_factor': %d }", replicationFactor));
  }

  public static Replication networkTopologyStrategy(
      Map<String, Integer> dataCenterReplicationFactors) {
    StringBuilder sb = new StringBuilder();
    sb.append("{ 'class' : 'NetworkTopologyStrategy'");
    for (Map.Entry<String, Integer> entry : dataCenterReplicationFactors.entrySet()) {
      String dcName = entry.getKey();
      int dcReplication = entry.getValue();
      sb.append(", '").append(dcName).append("': ").append(dcReplication);
    }
    sb.append(" }");
    return new Replication(sb.toString());
  }

  @Override
  public String toString() {
    return replication;
  }
}
