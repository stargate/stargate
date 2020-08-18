package io.stargate.db.datastore.common;

import java.net.InetAddress;

import org.apache.cassandra.locator.GossipingPropertyFileSnitch;
import org.apache.cassandra.utils.FBUtilities;

public class StargateConfigSnitch extends GossipingPropertyFileSnitch
{
    String dc = System.getProperty("stargate.datacenter", "DEFAULT_DC");
    String rack = System.getProperty("stargate.rack", "DEFAULT_RACK");

    @Override
    public String getRack(InetAddress inetAddress)
    {
        if (inetAddress.equals(FBUtilities.getBroadcastAddress()))
        {
            return rack;
        }

        return super.getRack(inetAddress);
    }

    @Override
    public String getDatacenter(InetAddress inetAddress)
    {
        if (inetAddress.equals(FBUtilities.getBroadcastAddress()))
        {
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
