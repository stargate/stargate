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
package io.stargate.db.cassandra.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.UnknownHostException;
import java.util.UUID;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.Gossiper.Props;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Cassandra50Persistence}. See also integration tests: {@link
 * Cassandra50PersistenceIT}.
 */
class Cassandra50PersistenceTest {
  private static final IPartitioner partitioner = Murmur3Partitioner.instance;
  private static final VersionedValue.VersionedValueFactory valueFactory =
      new VersionedValue.VersionedValueFactory(partitioner);

  private static final UUID id1 = UUID.randomUUID();
  private static final UUID id2 = UUID.randomUUID();

  private static final Cassandra50Persistence persistence = new Cassandra50Persistence();

  private static InetAddressAndPort storage1;
  private static InetAddressAndPort storage2;
  private static InetAddressAndPort stargate1;
  private static InetAddressAndPort local;

  @BeforeAll
  public static void setup() throws UnknownHostException {
    System.setProperty(Props.DISABLE_THREAD_VALIDATION, "true");

    DatabaseDescriptor.daemonInitialization();
    Gossiper.instance.stop();

    local = FBUtilities.getLocalAddressAndPort();
    stargate1 = InetAddressAndPort.getByName("127.1.0.10");
    storage1 = InetAddressAndPort.getByName("127.2.0.1");
    storage2 = InetAddressAndPort.getByName("127.2.0.2");

    Gossiper.instance.initializeNodeUnsafe(local, UUID.randomUUID(), 1);
    Gossiper.instance.initializeNodeUnsafe(stargate1, UUID.randomUUID(), 1);
    Gossiper.instance.initializeNodeUnsafe(storage1, UUID.randomUUID(), 1);
    Gossiper.instance.initializeNodeUnsafe(storage2, UUID.randomUUID(), 1);

    // This end point will have a null return value from Gossiper.getEndpointStateForEndpoint()
    Gossiper.instance.realMarkAlive(
        InetAddressAndPort.getByName("127.1.0.20"),
        Gossiper.instance.getEndpointStateForEndpoint(stargate1));

    TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
    tokenMetadata.clearUnsafe();
    tokenMetadata.updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), storage1);
    tokenMetadata.updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), storage2);
  }

  @AfterAll
  public static void shutdown() {
    Gossiper.instance.stop();
  }

  private static void live(InetAddressAndPort node, UUID schema) {
    Gossiper.instance.injectApplicationState(
        node, ApplicationState.SCHEMA, valueFactory.schema(schema));
    Gossiper.instance.injectApplicationState(
        node, ApplicationState.STATUS_WITH_PORT, valueFactory.load(1));
    Gossiper.instance.realMarkAlive(node, Gossiper.instance.getEndpointStateForEndpoint(node));
  }

  private static void dead(InetAddressAndPort node, UUID schema) {
    Gossiper.instance.injectApplicationState(
        node, ApplicationState.SCHEMA, valueFactory.schema(schema));
    Gossiper.instance.injectApplicationState(
        node, ApplicationState.STATUS_WITH_PORT, valueFactory.hibernate(true));
    Gossiper.instance.markDead(node, Gossiper.instance.getEndpointStateForEndpoint(node));
  }

  @Test
  public void testIsInSchemaAgreement() {
    live(local, id1);
    live(stargate1, id1);
    live(storage1, id1);
    live(storage2, id1);

    assertThat(persistence.isInSchemaAgreement()).isTrue();
    assertThat(persistence.isInSchemaAgreementWithStorage()).isTrue();
    assertThat(persistence.isStorageInSchemaAgreement()).isTrue();
    assertThat(persistence.isSchemaAgreementAchievable()).isTrue();
  }

  @Test
  public void schemaAgreementWithDeadNodes() {
    live(local, id1);
    dead(stargate1, id2);
    dead(storage1, id2);
    live(storage2, id1);

    assertThat(persistence.isInSchemaAgreement()).isTrue();
    assertThat(persistence.isInSchemaAgreementWithStorage()).isTrue();
    assertThat(persistence.isStorageInSchemaAgreement()).isTrue();
    assertThat(persistence.isSchemaAgreementAchievable()).isTrue();
  }

  @Test
  public void schemaDisagreementWithStargate() {
    live(local, id1);
    live(stargate1, id2);
    live(storage1, id1);
    live(storage2, id1);

    assertThat(persistence.isInSchemaAgreement()).isFalse();
    assertThat(persistence.isInSchemaAgreementWithStorage()).isTrue();
    assertThat(persistence.isStorageInSchemaAgreement()).isTrue();
    assertThat(persistence.isSchemaAgreementAchievable()).isTrue();
  }

  @Test
  public void schemaDisagreementWithStorage() {
    live(local, id1);
    live(stargate1, id1);
    live(storage1, id2);
    live(storage2, id2);

    assertThat(persistence.isInSchemaAgreement()).isFalse();
    assertThat(persistence.isInSchemaAgreementWithStorage()).isFalse();
    assertThat(persistence.isStorageInSchemaAgreement()).isTrue();
    assertThat(persistence.isSchemaAgreementAchievable()).isTrue();
  }

  @Test
  public void schemaDisagreementAmongStorageNodes() {
    live(local, id1);
    live(stargate1, id1);
    live(storage1, id1);
    live(storage2, id2);

    assertThat(persistence.isInSchemaAgreement()).isFalse();
    assertThat(persistence.isInSchemaAgreementWithStorage()).isFalse();
    assertThat(persistence.isStorageInSchemaAgreement()).isFalse();
    assertThat(persistence.isSchemaAgreementAchievable()).isTrue();
  }

  @Test
  public void schemaDisagreementLocalDead() {
    dead(local, id2);
    live(stargate1, id1);
    live(storage1, id1);
    live(storage2, id1);

    assertThat(persistence.isInSchemaAgreement()).isTrue();
    assertThat(persistence.isInSchemaAgreementWithStorage()).isTrue();
    assertThat(persistence.isStorageInSchemaAgreement()).isTrue();
    assertThat(persistence.isSchemaAgreementAchievable()).isTrue();
  }
}
