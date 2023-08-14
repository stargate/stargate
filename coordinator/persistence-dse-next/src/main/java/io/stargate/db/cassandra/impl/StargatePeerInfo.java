package io.stargate.db.cassandra.impl;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Throwables;

public class StargatePeerInfo extends StargateNodeInfo {
  private final InetAddressAndPort peer;

  public StargatePeerInfo(InetAddressAndPort peer) {
    this.peer = peer;
    setTokens(StargateSystemKeyspace.generateRandomTokens(peer, DatabaseDescriptor.getNumTokens()));
  }

  public InetAddressAndPort getPeer() {
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
