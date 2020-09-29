package io.stargate.db.cassandra.impl;

import org.apache.cassandra.locator.GossipingPropertyFileSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

public class StargateConfigSnitch extends GossipingPropertyFileSnitch {
  String dc = System.getProperty("stargate.datacenter", "UNKNOWN_DC");
  String rack = System.getProperty("stargate.rack", "UNKNOWN_RACK");

  @Override
  public String getRack(InetAddressAndPort inetAddress) {
    if (inetAddress.equals(FBUtilities.getBroadcastAddressAndPort())) {
      return rack;
    }

    return super.getRack(inetAddress);
  }

  @Override
  public String getDatacenter(InetAddressAndPort inetAddress) {
    if (inetAddress.equals(FBUtilities.getBroadcastAddressAndPort())) {
      return dc;
    }

    return super.getDatacenter(inetAddress);
  }

  public String getLocalDatacenter() {
    return this.dc;
  }

  public String getLocalRack() {
    return this.rack;
  }

  public String toString() {
    return "StargateConfigSnitch{myDC='" + this.dc + '\'' + ", myRack='" + this.rack + "'}";
  }
}
