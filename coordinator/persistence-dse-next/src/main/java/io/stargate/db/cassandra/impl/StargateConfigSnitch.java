package io.stargate.db.cassandra.impl;

import io.stargate.db.datastore.common.StargateSnitchProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.GossipingPropertyFileSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The Stargate extension of the {@link GossipingPropertyFileSnitch}.
 *
 * <p><i>Note that similar copy of this class exists in all persistence implementations, thus please
 * apply changes to all of them if needed.</i>
 */
public class StargateConfigSnitch extends GossipingPropertyFileSnitch {

  private final StargateSnitchProperties snitchProperties;

  public StargateConfigSnitch() throws ConfigurationException {
    this(new StargateSnitchProperties("UNKNOWN_DC", "UNKNOWN_RACK"));
  }

  public StargateConfigSnitch(StargateSnitchProperties snitchProperties)
      throws ConfigurationException {
    this.snitchProperties = snitchProperties;
  }

  @Override
  public String getRack(InetAddressAndPort inetAddress) {
    if (inetAddress.equals(FBUtilities.getBroadcastAddressAndPort())) {
      return snitchProperties.getRack();
    }

    return super.getRack(inetAddress);
  }

  @Override
  public String getDatacenter(InetAddressAndPort inetAddress) {
    if (inetAddress.equals(FBUtilities.getBroadcastAddressAndPort())) {
      return snitchProperties.getDc();
    }

    return super.getDatacenter(inetAddress);
  }

  @Override
  public String getLocalDatacenter() {
    return snitchProperties.getDc();
  }

  @Override
  public String getLocalRack() {
    return snitchProperties.getRack();
  }

  @Override
  public String toString() {
    return "StargateConfigSnitch{DC='"
        + snitchProperties.getDc()
        + '\''
        + ", rack='"
        + snitchProperties.getRack()
        + "'}";
  }
}
