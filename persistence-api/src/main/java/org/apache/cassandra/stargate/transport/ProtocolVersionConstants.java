package org.apache.cassandra.stargate.transport;

public class ProtocolVersionConstants {
  public static String clusterVersion = System.getProperty("stargate.cluster_version", "6.8");
  public static String V5_STR = clusterVersion.equals("4.0") ? "V5" : "v5-beta";
  public static boolean V5_BETA = !clusterVersion.equals("4.0");
}
