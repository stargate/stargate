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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.db.datastore.common;

import java.net.InetAddress;
import org.apache.cassandra.locator.GossipingPropertyFileSnitch;
import org.apache.cassandra.utils.FBUtilities;

public class StargateConfigSnitch extends GossipingPropertyFileSnitch {
  String dc = System.getProperty("stargate.datacenter", "DEFAULT_DC");
  String rack = System.getProperty("stargate.rack", "DEFAULT_RACK");

  @Override
  public String getRack(InetAddress inetAddress) {
    if (inetAddress.equals(FBUtilities.getBroadcastAddress())) {
      return rack;
    }

    return super.getRack(inetAddress);
  }

  @Override
  public String getDatacenter(InetAddress inetAddress) {
    if (inetAddress.equals(FBUtilities.getBroadcastAddress())) {
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

  @Override
  public String toString() {
    return "StargateConfigSnitch{myDC='" + this.dc + '\'' + ", myRack='" + this.rack + "'}";
  }
}
