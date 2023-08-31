package io.stargate.db.cassandra.impl;

import java.net.InetAddress;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Throwables;

public class StargateLocalInfo extends StargateNodeInfo {
  private volatile InetAddress broadcastAddress;
  private volatile String clusterName;
  private volatile CassandraVersion cqlVersion;
  private volatile InetAddress listenAddress;
  private volatile String nativeProtocolVersion;
  private volatile String partitioner;

  public InetAddress getBroadcastAddress() {
    return broadcastAddress;
  }

  public void setBroadcastAddress(InetAddress broadcastAddress) {
    this.broadcastAddress = broadcastAddress;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public CassandraVersion getCqlVersion() {
    return cqlVersion;
  }

  public void setCqlVersion(CassandraVersion cqlVersion) {
    this.cqlVersion = cqlVersion;
  }

  public InetAddress getListenAddress() {
    return listenAddress;
  }

  public void setListenAddress(InetAddress listenAddress) {
    this.listenAddress = listenAddress;
  }

  public String getNativeProtocolVersion() {
    return nativeProtocolVersion;
  }

  public void setNativeProtocolVersion(String nativeProtocolVersion) {
    this.nativeProtocolVersion = nativeProtocolVersion;
  }

  public String getPartitioner() {
    return partitioner;
  }

  public void setPartitioner(String partitioner) {
    this.partitioner = partitioner;
  }

  @Override
  public StargateLocalInfo copy() {
    try {
      return (StargateLocalInfo) clone();
    } catch (CloneNotSupportedException e) {
      throw Throwables.unchecked(e);
    }
  }
}
