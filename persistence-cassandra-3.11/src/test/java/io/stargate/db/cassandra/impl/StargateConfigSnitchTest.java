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

package io.stargate.db.cassandra.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StargateConfigSnitchTest {

  @AfterEach
  public void clear() {
    System.clearProperty("stargate.datacenter");
    System.clearProperty("stargate.rack");
  }

  @Nested
  class GetRack {

    @Test
    public void defaultRack() throws Exception {
      InetAddress localAddress = InetAddress.getLocalHost();
      FBUtilities.setBroadcastInetAddress(localAddress);

      StargateConfigSnitch configSnitch = new StargateConfigSnitch();
      String result = configSnitch.getRack(localAddress);

      assertThat(result).isEqualTo("DEFAULT_RACK");
    }

    @Test
    public void configurationRack() throws Exception {
      InetAddress localAddress = InetAddress.getLocalHost();
      FBUtilities.setBroadcastInetAddress(localAddress);
      System.setProperty("stargate.rack", "test_rack");

      StargateConfigSnitch configSnitch = new StargateConfigSnitch();
      String result = configSnitch.getRack(localAddress);

      assertThat(result).isEqualTo("test_rack");
    }
  }

  @Nested
  class GetDatacenter {

    @Test
    public void defaultDc() throws Exception {
      InetAddress localAddress = InetAddress.getLocalHost();
      FBUtilities.setBroadcastInetAddress(localAddress);

      StargateConfigSnitch configSnitch = new StargateConfigSnitch();
      String result = configSnitch.getDatacenter(localAddress);

      assertThat(result).isEqualTo("DEFAULT_DC");
    }

    @Test
    public void configurationDc() throws Exception {
      InetAddress localAddress = InetAddress.getLocalHost();
      FBUtilities.setBroadcastInetAddress(localAddress);
      System.setProperty("stargate.datacenter", "test_dc");

      StargateConfigSnitch configSnitch = new StargateConfigSnitch();
      String result = configSnitch.getDatacenter(localAddress);

      assertThat(result).isEqualTo("test_dc");
    }
  }
}
