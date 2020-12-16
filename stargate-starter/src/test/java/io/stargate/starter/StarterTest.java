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
package io.stargate.starter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StarterTest {

  @BeforeEach
  public void reset() {
    System.setProperties(null);
  }

  @Test
  void testSetStargatePropertiesWithIPSeedNode() {
    Starter starter = setupDefaultStarter();

    starter.setStargateProperties();

    assertThat(System.getProperty("stargate.seed_list")).isEqualTo("127.0.0.1,127.0.0.2");
  }

  @Test
  void testSetStargatePropertiesWithHostSeedNode() {
    Starter starter = setupDefaultStarter();
    starter.seedList = Arrays.asList("cassandra.apache.org", "datastax.com");

    starter.setStargateProperties();

    assertThat(System.getProperty("stargate.seed_list"))
        .isEqualTo("cassandra.apache.org,datastax.com");
  }

  @Test
  void testSetStargatePropertiesWithBadHostSeedNode() {
    Starter starter = setupDefaultStarter();
    starter.seedList = Arrays.asList("google.com", "datasta.cmo", "cassandra.apache.org");

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            starter::setStargateProperties,
            "Expected setStargateProperties() to throw RuntimeException");

    assertThat(System.getProperty("stargate.seed_list")).isNull();
    assertThat(thrown.getMessage()).isEqualTo("Unable to resolve seed node address datasta.cmo");
  }

  @Test
  void testSetStargatePropertiesMissingSeedNode() {
    Starter starter = setupDefaultStarter();
    starter.seedList = new ArrayList<>();

    RuntimeException thrown =
        assertThrows(
            IllegalArgumentException.class,
            starter::setStargateProperties,
            "Expected setStargateProperties() to throw RuntimeException");

    assertThat(System.getProperty("stargate.seed_list")).isNull();
    assertThat(thrown.getMessage()).isEqualTo("At least one seed node address is required.");
  }

  @Test
  void testSetStargatePropertiesMissingDC() {
    Starter starter = setupDefaultStarter();
    starter.simpleSnitch = false;
    starter.rack = "rack0";

    RuntimeException thrown =
        assertThrows(
            IllegalArgumentException.class,
            starter::setStargateProperties,
            "Expected setStargateProperties() to throw RuntimeException");

    assertThat(thrown.getMessage())
        .isEqualTo("--dc and --rack are both required unless --simple-snitch is specified.");
  }

  @Test
  void testSetStargatePropertiesMissingRack() {
    Starter starter = setupDefaultStarter();
    starter.simpleSnitch = false;
    starter.dc = "dc1";

    RuntimeException thrown =
        assertThrows(
            IllegalArgumentException.class,
            starter::setStargateProperties,
            "Expected setStargateProperties() to throw RuntimeException");

    assertThat(thrown.getMessage())
        .isEqualTo("--dc and --rack are both required unless --simple-snitch is specified.");
  }

  @Test
  void testSetStargatePropertiesMissingVersion() {
    Starter starter = setupDefaultStarter();
    starter.version = null;

    RuntimeException thrown =
        assertThrows(
            IllegalArgumentException.class,
            starter::setStargateProperties,
            "Expected setStargateProperties() to throw RuntimeException");

    assertThat(thrown.getMessage()).isEqualTo("--cluster-version must be a number");
  }

  @Test
  void testSeedsNotPresentThrows() {
    Starter starter = setupDefaultStarter();
    starter.seedList = Collections.emptyList();

    assertThrows(
        IllegalArgumentException.class,
        starter::setStargateProperties,
        "At least one seed node address is required.");
  }

  @Test
  void testDeveloperModeSetsDefaultSeedsAndSnitch() {
    Starter starter = setupDefaultStarter();
    starter.developerMode = true;
    starter.simpleSnitch = false;
    starter.seedList = new ArrayList<>();

    starter.setStargateProperties();

    assertThat(System.getProperty("stargate.seed_list")).isEqualTo("127.0.0.1");
    assertThat(System.getProperty("stargate.developer_mode")).isEqualTo("true");
    assertThat(System.getProperty("stargate.snitch_classname")).isEqualTo("SimpleSnitch");
  }

  @Test
  void testSetStargatePropertiesWithIPv4ListenHost() {
    Starter starter = setupDefaultStarter();
    starter.listenHostStr = "127.0.0.1";

    starter.setStargateProperties();

    assertThat(System.getProperty("stargate.listen_address")).isEqualTo("127.0.0.1");
  }

  @Test
  void testSetStargatePropertiesWithInvalidIPv4ListenHost() {
    Starter starter = setupDefaultStarter();
    starter.listenHostStr = "127.0.999.1";

    RuntimeException thrown =
        assertThrows(
            IllegalArgumentException.class,
            starter::setStargateProperties,
            "Expected setStargateProperties() to throw RuntimeException");

    assertThat(thrown.getMessage()).isEqualTo("--listen must be a valid IPv4 or IPv6 address");
  }

  @Test
  void testSetStargatePropertiesWithIPv6ListenHost() {
    Starter starter = setupDefaultStarter();
    starter.listenHostStr = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";

    starter.setStargateProperties();

    assertThat(System.getProperty("stargate.listen_address"))
        .isEqualTo("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
  }

  @Test
  void testSetStargatePropertiesWithInvalidIPv6ListenHost() {
    Starter starter = setupDefaultStarter();
    starter.listenHostStr = "2001:0db8:85a3:x:0000:8a2e:0370:7334";

    RuntimeException thrown =
        assertThrows(
            IllegalArgumentException.class,
            starter::setStargateProperties,
            "Expected setStargateProperties() to throw RuntimeException");

    assertThat(thrown.getMessage()).isEqualTo("--listen must be a valid IPv4 or IPv6 address");
  }

  private Starter setupDefaultStarter() {
    Starter starter = new Starter();
    starter.simpleSnitch = true;
    starter.clusterName = "foo";
    starter.version = "3.11";
    starter.seedList = Arrays.asList("127.0.0.1", "127.0.0.2");

    return starter;
  }
}
