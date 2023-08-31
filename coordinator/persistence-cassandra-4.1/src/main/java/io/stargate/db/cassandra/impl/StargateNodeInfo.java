package io.stargate.db.cassandra.impl;

import java.net.InetAddress;
import java.util.Set;
import java.util.UUID;

public abstract class StargateNodeInfo implements Cloneable {
  private volatile UUID hostId;
  private volatile String dataCenter;
  private volatile String rack;
  private volatile String releaseVersion;
  private volatile InetAddress nativeAddress;
  private volatile Integer nativePort;
  private volatile Set<String> tokens;

  public UUID getHostId() {
    return hostId;
  }

  public void setHostId(UUID hostId) {
    this.hostId = hostId;
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public void setDataCenter(String dataCenter) {
    this.dataCenter = dataCenter;
  }

  public String getRack() {
    return rack;
  }

  public void setRack(String rack) {
    this.rack = rack;
  }

  public String getReleaseVersion() {
    return releaseVersion;
  }

  public void setReleaseVersion(String releaseVersion) {
    this.releaseVersion = releaseVersion;
  }

  public InetAddress getRpcAddress() {
    return nativeAddress;
  }

  public InetAddress getNativeAddress() {
    return nativeAddress;
  }

  public void setNativeAddress(InetAddress nativeAddress) {
    this.nativeAddress = nativeAddress;
  }

  public Integer getNativePort() {
    return nativePort;
  }

  public Integer getRpcPort() {
    return nativePort;
  }

  public void setNativePort(Integer nativePort) {
    this.nativePort = nativePort;
  }

  public Set<String> getTokens() {
    return tokens;
  }

  public void setTokens(Set<String> tokens) {
    this.tokens = tokens;
  }

  public abstract StargateNodeInfo copy();
}
