package io.stargate.db.dse.impl;

import java.net.InetAddress;
import org.apache.cassandra.utils.Throwables;

public class StargatePeerInfo extends StargateNodeInfo {
  private final InetAddress peer;

  public StargatePeerInfo(InetAddress peer) {
    this.peer = peer;
  }

  public InetAddress getPeer() {
    return peer;
  }

  @Override
  public StargatePeerInfo copy() {
    try {
      return (StargatePeerInfo) clone();
    } catch (CloneNotSupportedException e) {
      throw Throwables.unchecked(e);
    }
  }
}
