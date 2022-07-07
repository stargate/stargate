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
package io.stargate.it.http.graphql.graphqlfirst;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class SchemaDeploymentTest extends GraphqlFirstTestBase {

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
  public void cleanupDb(@TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    deleteAllGraphqlSchemas(keyspaceId.asInternal(), session);
    session.execute("DROP TABLE IF EXISTS  \"User\"");
  }

  @Test
  @DisplayName("Should deploy schema and set the deployment_in_progress column to null")
  public void deploySchemaAndSetDeploymentInProgressToNull(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    String keyspace = keyspaceId.asInternal();

    // when
    CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);

    // then
    Row row =
        session
            .execute(
                "select * from stargate_graphql.schema_source wHERE keyspace_name = ?", keyspace)
            .one();
    assertThat(row).isNotNull();
    assertThat(row.isNull("deployment_in_progress")).isFalse();
    assertThat(row.getBoolean("deployment_in_progress")).isFalse();
  }

  @Test
  @DisplayName("Should fail to deploy schema_source when already in progress")
  public void deploySchemaWhenInProgress(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    String keyspace = keyspaceId.asInternal();
    UUID currentVersion = CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);
    session.execute(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = ?",
        keyspace);

    // when
    String error =
        CLIENT.getDeploySchemaError(keyspace, currentVersion.toString(), SCHEMA_CONTENTS);

    // then
    assertThat(error).contains("It looks like someone else is deploying a new schema");
  }

  @Test
  @DisplayName("Should force deployment when already in progress")
  public void forceDeploySchemaWhenInProgress(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    String keyspace = keyspaceId.asInternal();
    UUID currentVersion = CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);
    session.execute(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = ?",
        keyspace);

    // when
    CLIENT.deploySchema(keyspace, currentVersion.toString(), true, SCHEMA_CONTENTS);

    // then
    Row row =
        session
            .execute(
                "select * from stargate_graphql.schema_source wHERE keyspace_name = ?", keyspace)
            .one();
    assertThat(row).isNotNull();
    assertThat(row.isNull("deployment_in_progress")).isFalse();
    assertThat(row.getBoolean("deployment_in_progress")).isFalse();
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
  @DisplayName("Should fail to force deploy schema when version doesn't match")
  public void forceDeploySchemaWhenVersionMismatch(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    String keyspace = keyspaceId.asInternal();
    UUID currentVersion = CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);
    session.execute(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = ?",
        keyspace);
    UUID wrongExpectedVersion = Uuids.timeBased();
    assertThat(wrongExpectedVersion).isNotEqualTo(currentVersion);

    // when
    String error =
        CLIENT.getDeploySchemaError(
            keyspace, wrongExpectedVersion.toString(), true, SCHEMA_CONTENTS);

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
  @DisplayName(
      "Should purge older schema entries, keeping only last SchemaSourceDao#NUMBER_OF_RETAINED_SCHEMA_VERSIONS versions")
  public void purgeOldSchemaEntriesOnInsert(@TestKeyspace CqlIdentifier keyspaceId) {
    // given inserted NUMBER_OF_RETAINED_SCHEMA_VERSIONS + N schemas
    String keyspace = keyspaceId.asInternal();
    int numberOfSchemasAboveThreshold = 5;
    int numberOfVersionsToInsert =
        NUMBER_OF_RETAINED_SCHEMA_VERSIONS + numberOfSchemasAboveThreshold;

    // when deploying schemas
    List<UUID> schemasVersions = new ArrayList<>();
    UUID lastVersion = null;
    for (int i = 0; i < numberOfVersionsToInsert; i++) {
      lastVersion =
          CLIENT.deploySchema(
              keyspace, lastVersion == null ? null : lastVersion.toString(), SCHEMA_CONTENTS);
      schemasVersions.add(lastVersion);
    }

    // then the last NUMBER_OF_RETAINED_SCHEMA_VERSIONS schemas should be present
    List<UUID> removedVersions = schemasVersions.subList(0, numberOfSchemasAboveThreshold);
    List<UUID> presentVersions =
        schemasVersions.subList(numberOfSchemasAboveThreshold, schemasVersions.size());
    // all removed versions should return 404
    for (UUID version : removedVersions) {
      CLIENT.getSchemaFile(keyspace, version.toString(), Response.Status.NOT_FOUND.getStatusCode());
    }
    // rest of the schemas should be present
    for (UUID version : presentVersions) {
      assertThat(CLIENT.getSchemaFile(keyspace, version.toString())).isNotNull();
    }
  }

  @Test
  @DisplayName("Should undeploy schema")
  public void undeploySchema(@TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    String keyspace = keyspaceId.asInternal();
    UUID version1 = CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);
    UUID version2 = CLIENT.deploySchema(keyspace, version1.toString(), SCHEMA_CONTENTS);

    // when
    CLIENT.undeploySchema(keyspace, version2.toString());

    // then
    // rows are still here but none is marked as latest
    List<Row> rows =
        session
            .execute(
                "select * from stargate_graphql.schema_source where keyspace_name = ?", keyspace)
            .all();
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getUuid("latest_version")).isNull();
  }

  @Test
  @DisplayName("Should redeploy schema after undeployment")
  public void undeployAndRedeploySchema(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    String keyspace = keyspaceId.asInternal();
    UUID version1 = CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);
    UUID version2 = CLIENT.deploySchema(keyspace, version1.toString(), SCHEMA_CONTENTS);
    CLIENT.undeploySchema(keyspace, version2.toString());

    // when
    // we don't require the previous version here, because it's considered inactive
    UUID version3 = CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);

    // then
    List<Row> rows =
        session
            .execute(
                "select * from stargate_graphql.schema_source where keyspace_name = ?", keyspace)
            .all();
    assertThat(rows).hasSize(3);
    assertThat(rows.get(0).getUuid("latest_version")).isEqualTo(version3);
  }

  @Test
  @DisplayName("Should fail to undeploy schema when version doesn't match")
  public void undeploySchemaWhenVersionMismatch(@TestKeyspace CqlIdentifier keyspaceId) {
    // given
    String keyspace = keyspaceId.asInternal();
    UUID version = CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);
    UUID wrongVersion = Uuids.timeBased();
    assertThat(wrongVersion).isNotEqualTo(version);

    // when
    String error = CLIENT.getUndeploySchemaError(keyspace, wrongVersion.toString());

    // then
    assertThat(error)
        .contains(
            String.format(
                "You specified expectedVersion %s, but there is a more recent version %s",
                wrongVersion, version));
  }

  @Test
  @DisplayName("Should fail to undeploy schema when current still in progress")
  public void undeploySchemaWhenInProgress(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    String keyspace = keyspaceId.asInternal();
    UUID version = CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);
    session.execute(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = ?",
        keyspace);

    // when
    String error = CLIENT.getUndeploySchemaError(keyspace, version.toString());

    // then
    assertThat(error).contains("It looks like someone else is deploying a new schema");
  }

  @Test
  @DisplayName("Should force undeploy schema when current still in progress")
  public void forceUndeploySchemaWhenInProgress(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    String keyspace = keyspaceId.asInternal();
    UUID version = CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);
    session.execute(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = ?",
        keyspace);

    // when
    CLIENT.undeploySchema(keyspace, version.toString(), true);
    Row row =
        session
            .execute(
                "SELECT latest_version, deployment_in_progress "
                    + "FROM stargate_graphql.schema_source "
                    + "WHERE keyspace_name = ?",
                keyspace)
            .one();

    // then
    assertThat(row).isNotNull();
    assertThat(row.getUuid("latest_version")).isNull();
    assertThat(row.getBoolean("deployment_in_progress")).isFalse();
  }

  @Test
  @DisplayName("Should fail to force undeploy schema when version doesn't match")
  public void forceUndeploySchemaWhenVersionMismatch(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // given
    String keyspace = keyspaceId.asInternal();
    UUID version = CLIENT.deploySchema(keyspace, SCHEMA_CONTENTS);
    session.execute(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = ?",
        keyspace);
    UUID wrongVersion = Uuids.timeBased();
    assertThat(wrongVersion).isNotEqualTo(version);

    // when
    String error = CLIENT.getUndeploySchemaError(keyspace, wrongVersion.toString());

    // then
    assertThat(error)
        .contains(
            String.format(
                "You specified expectedVersion %s, but there is a more recent version %s",
                wrongVersion, version));
  }

  @Test
  @DisplayName("Should not include stacktrace in error response")
  public void deploySchemaErrorNoStacktrace(@TestKeyspace CqlIdentifier keyspaceId) {
    // given
    String invalidSchema = "type Foo { id ID }"; // missing colon before `ID`

    // when
    List<Map<String, Object>> errors =
        CLIENT.getDeploySchemaErrors(keyspaceId.asInternal(), null, invalidSchema);

    // then
    assertThat(errors).hasSize(1);
    Map<String, Object> schemaError = JsonPath.read(errors.get(0), "$.extensions.schemaErrors[0]");
    assertThat(schemaError.get("message"))
        .asInstanceOf(InstanceOfAssertFactories.STRING)
        .contains(
            "The schema definition text contains a non schema definition language (SDL) element "
                + "'OperationDefinition'");
    // The error is a NonSDLDefinitionError, which also implements java.lang.Exception. By default
    // the GraphQL engine formats it with the full stacktrace, ensure that we explicitly convert it
    // to the spec's format to avoid that:
    assertThat(schemaError).doesNotContainKey("stackTrace");
  }

  @Test
  @DisplayName("Should return null response when no schema has been deployed yet")
  public void noDeployedSchema(@TestKeyspace CqlIdentifier keyspaceId) {
    // When
    Object response =
        CLIENT.executeAdminQuery(
            String.format(
                "{ schema(keyspace: \"%s\") { deployDate, contents} }", keyspaceId.asInternal()));

    // Then
    assertThat(JsonPath.<Object>read(response, "$.schema")).isNull();
  }
}
