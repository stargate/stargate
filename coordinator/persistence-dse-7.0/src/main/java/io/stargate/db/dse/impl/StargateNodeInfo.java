package io.stargate.db.dse.impl;

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
  private volatile Integer nativePortSsl;
  private volatile Integer jmxPort;
  private volatile Integer storagePort;
  private volatile Integer storagePortSsl;
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

  public void setNativePort(Integer nativePort) {
    this.nativePort = nativePort;
  }

  public Integer getJmxPort() {
    return jmxPort;
  }

  public void setJmxPort(Integer jmxPort) {
    this.jmxPort = jmxPort;
  }

  public Integer getStoragePort() {
    return storagePort;
  }

  public void setStoragePort(Integer storagePort) {
    this.storagePort = storagePort;
  }

  public Integer getStoragePortSsl() {
    return storagePortSsl;
  }

  public void setStoragePortSsl(Integer storagePortSsl) {
    this.storagePortSsl = storagePortSsl;
  }

  public Integer getNativePortSsl() {
    return nativePortSsl;
  }

  public void setNativePortSsl(Integer nativePortSsl) {
    this.nativePortSsl = nativePortSsl;
  }

  public Set<String> getTokens() {
    return tokens;
  }

  public void setTokens(Set<String> tokens) {
    this.tokens = tokens;
  }

  public abstract StargateNodeInfo copy();
}
