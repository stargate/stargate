/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.db.dse.impl;

import io.stargate.db.datastore.common.StargateSnitchProperties;
import java.net.InetAddress;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.GossipingPropertyFileSnitch;
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
    this(new StargateSnitchProperties("DEFAULT_DC", "DEFAULT_RACK"));
  }

  public StargateConfigSnitch(StargateSnitchProperties snitchProperties)
      throws ConfigurationException {
    this.snitchProperties = snitchProperties;
  }

  @Override
  public String getRack(InetAddress inetAddress) {
    if (inetAddress.equals(FBUtilities.getBroadcastAddress())) {
      return snitchProperties.getRack();
    }

    return super.getRack(inetAddress);
  }

  @Override
  public String getDatacenter(InetAddress inetAddress) {
    if (inetAddress.equals(FBUtilities.getBroadcastAddress())) {
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
    return "StargateConfigSnitch{myDC='"
        + snitchProperties.getDc()
        + '\''
        + ", myRack='"
        + snitchProperties.getRack()
        + "'}";
  }
}
