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
package io.stargate.it.http.graphql.graphfirst;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class SchemaDeploymentTest extends BaseOsgiIntegrationTest {

  private static final int NUMBER_OF_RETAINED_SCHEMA_VERSIONS = 10;

  private static final String SCHEMA_CONTENTS =
      "type User { id: ID! name: String username: String } "
          + "type Query { getUser(id: ID!): User }";

  private static GraphqlFirstClient CLIENT;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) {
    String host = cluster.seedAddress();
    CLIENT = new GraphqlFirstClient(host, RestUtils.getAuthToken(host));
  }

  @BeforeEach
  public void cleanupDb(CqlSession session) {
    session.execute("DROP TABLE IF EXISTS graphql_schema");
    session.execute("DROP TABLE IF EXISTS  \"User\"");
  }

  @Test
  @DisplayName("Should deploy schema and set the deployment_in_progress column to null")
  public void deploySchemaAndSetDeploymentInProgressToNull(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // when
    CLIENT.deploySchema(keyspaceId.asInternal(), SCHEMA_CONTENTS);

    // then
    Row row = session.execute("select * from graphql_schema").one();
    assertThat(row).isNotNull();
    assertThat(row.isNull("deployment_in_progress")).isFalse();
    assertThat(row.getBoolean("deployment_in_progress")).isFalse();
  }

  @Test
  @DisplayName("Should fail to deploy schema when already in progress")
  public void deploySchemaWhenInProgress(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    UUID currentVersion = CLIENT.deploySchema(keyspaceId.asInternal(), SCHEMA_CONTENTS);
    session.execute("UPDATE graphql_schema SET deployment_in_progress = true WHERE key = 'key'");

    // when
    String error =
        CLIENT.getDeploySchemaError(
            keyspaceId.asInternal(), currentVersion.toString(), SCHEMA_CONTENTS);

    // then
    assertThat(error)
        .contains("It looks like someone else is deploying a new schema. Please try again later.");
  }

  @Test
  @DisplayName("Should fail to deploy schema when version doesn't match")
  public void deploySchemaWhenVersionMismatch(@TestKeyspace CqlIdentifier keyspaceId) {
    // given
    UUID currentVersion = CLIENT.deploySchema(keyspaceId.asInternal(), SCHEMA_CONTENTS);
    UUID wrongExpectedVersion = Uuids.timeBased();
    assertThat(wrongExpectedVersion).isNotEqualTo(currentVersion);

    // when
    String error =
        CLIENT.getDeploySchemaError(
            keyspaceId.asInternal(), wrongExpectedVersion.toString(), SCHEMA_CONTENTS);

    // then
    assertThat(error)
        .contains(
            String.format(
                "You specified expectedVersion %s, but there is a more recent version %s",
                wrongExpectedVersion, currentVersion));
  }

  @Test
  @DisplayName("Should fail to deploy schema when previous version expected but table is empty")
  public void deploySchemaWhenPreviousVersionExpectedButTableEmpty(
      @TestKeyspace CqlIdentifier keyspaceId) {
    // when
    UUID wrongExpectedVersion = Uuids.timeBased();
    String error =
        CLIENT.getDeploySchemaError(
            keyspaceId.asInternal(), wrongExpectedVersion.toString(), SCHEMA_CONTENTS);

    // then
    assertThat(error).contains("You specified expectedVersion but no previous version was found");
  }

  @Test
  @DisplayName("Should fail to deploy schema when table has the wrong structure")
  public void deploySchemaWhenWrongTableStructure(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    session.execute("CREATE TABLE graphql_schema(k int PRIMARY KEY)");

    // when
    String error = CLIENT.getDeploySchemaError(keyspaceId.asInternal(), null, SCHEMA_CONTENTS);

    // then
    assertThat(error)
        .contains(
            String.format(
                "Table '%s.graphql_schema' already exists, but it doesn't have the expected structure",
                keyspaceId.asInternal()));
  }

  @Test
  @DisplayName(
      "Should purge older schema entries, keeping only last SchemaSourceDao#NUMBER_OF_RETAINED_SCHEMA_VERSIONS versions")
  public void purgeOldSchemaEntriesOnInsert(@TestKeyspace CqlIdentifier keyspaceId) {
    // given inserted NUMBER_OF_RETAINED_SCHEMA_VERSIONS + N schemas
    String namespace = keyspaceId.asInternal();
    int numberOfSchemasAboveThreshold = 5;
    int numberOfVersionsToInsert =
        NUMBER_OF_RETAINED_SCHEMA_VERSIONS + numberOfSchemasAboveThreshold;

    // when deploying schemas
    List<UUID> schemasVersions = new ArrayList<>();
    UUID lastVersion = null;
    for (int i = 0; i < numberOfVersionsToInsert; i++) {
      lastVersion =
          CLIENT.deploySchema(
              namespace, lastVersion == null ? null : lastVersion.toString(), SCHEMA_CONTENTS);
      schemasVersions.add(lastVersion);
    }

    // then the last NUMBER_OF_RETAINED_SCHEMA_VERSIONS schemas should be present
    List<UUID> removedVersions = schemasVersions.subList(0, numberOfSchemasAboveThreshold);
    List<UUID> presentVersions =
        schemasVersions.subList(numberOfSchemasAboveThreshold, schemasVersions.size());
    // all removed versions should return 404
    for (UUID version : removedVersions) {
      CLIENT.getSchemaFile(
          namespace, version.toString(), Response.Status.NOT_FOUND.getStatusCode());
    }
    // rest of the schemas should be present
    for (UUID version : presentVersions) {
      assertThat(CLIENT.getSchemaFile(namespace, version.toString())).isNotNull();
    }
  }
}
