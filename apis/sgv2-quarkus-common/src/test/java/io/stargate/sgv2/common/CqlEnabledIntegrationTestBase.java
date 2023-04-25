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
package io.stargate.sgv2.common;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import java.time.Duration;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Base test class that exposes a Java driver {@link CqlSession} connected to the Stargate
 * coordinator, allowing subclasses to create and populate their CQL schema.
 *
 * <p>Subclasses must be annotated with {@link io.quarkus.test.junit.QuarkusIntegrationTest}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class CqlEnabledIntegrationTestBase {

  protected final CqlIdentifier keyspaceId =
      CqlIdentifier.fromInternal("ks" + RandomStringUtils.randomNumeric(16));

  protected CqlSession session;

  @BeforeAll
  public final void buildSession() {
    OptionsMap config = OptionsMap.driverDefaults();
    config.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10));
    config.put(TypedDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false);
    config.put(
        TypedDriverOption.LOAD_BALANCING_POLICY_CLASS,
        DcInferringLoadBalancingPolicy.class.getName());

    // resolve auth if enabled
    if (IntegrationTestUtils.isCassandraAuthEnabled()) {
      config.put(TypedDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class.getName());
      config.put(
          TypedDriverOption.AUTH_PROVIDER_USER_NAME, IntegrationTestUtils.getCassandraUsername());
      config.put(
          TypedDriverOption.AUTH_PROVIDER_PASSWORD, IntegrationTestUtils.getCassandraPassword());
    }

    session =
        CqlSession.builder()
            .addContactPoint(IntegrationTestUtils.getCassandraCqlAddress())
            .withConfigLoader(DriverConfigLoader.fromMap(config))
            .build();
  }

  @BeforeAll
  public final void createKeyspace() {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            .formatted(keyspaceId.asCql(false)));
    session.execute("USE %s".formatted(keyspaceId.asCql(false)));
  }

  @AfterAll
  public final void cleanUp() {
    if (session != null) {
      session.execute("DROP KEYSPACE IF EXISTS %s".formatted(keyspaceId.asCql(false)));
      session.close();
    }
  }
}
