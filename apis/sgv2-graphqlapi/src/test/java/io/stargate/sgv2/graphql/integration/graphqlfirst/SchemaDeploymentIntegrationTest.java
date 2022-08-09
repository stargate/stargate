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
package io.stargate.sgv2.graphql.integration.graphqlfirst;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.sgv2.api.common.grpc.proto.Rows;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import io.stargate.sgv2.graphql.schema.Uuids;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.enterprise.context.control.ActivateRequestContext;
import javax.ws.rs.core.Response;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SchemaDeploymentIntegrationTest extends GraphqlFirstIntegrationTest {

  private static final int NUMBER_OF_RETAINED_SCHEMA_VERSIONS = 10;

  private static final String SCHEMA_CONTENTS =
      "type User { id: ID! name: String username: String } "
          + "type Query { getUser(id: ID!): User }";

  @BeforeEach
  public void cleanupDb() {
    deleteAllGraphqlSchemas();
    executeCql("DROP TABLE IF EXISTS  \"User\"");
  }

  @Test
  @DisplayName("Should deploy schema and set the deployment_in_progress column to null")
  public void deploySchemaAndSetDeploymentInProgressToNull() {
    // when
    client.deploySchema(keyspaceName, SCHEMA_CONTENTS);

    // then
    ResultSet resultSet =
        executeCql(
                "select * from stargate_graphql.schema_source WHERE keyspace_name = '%s'"
                    .formatted(keyspaceName))
            .getResultSet();
    Row row = resultSet.getRows(0);
    assertThat(Rows.getBoolean(row, "deployment_in_progress", resultSet.getColumnsList()))
        .isFalse();
  }

  @Test
  @DisplayName("Should fail to deploy schema_source when already in progress")
  public void deploySchemaWhenInProgress() {
    // given
    UUID currentVersion = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);
    executeCql(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = '%s'"
                .formatted(keyspaceName));

    // when
    String error =
        client.getDeploySchemaError(keyspaceName, currentVersion.toString(), SCHEMA_CONTENTS);

    // then
    assertThat(error).contains("It looks like someone else is deploying a new schema");
  }

  @Test
  @DisplayName("Should force deployment when already in progress")
  public void forceDeploySchemaWhenInProgress() {
    // given

    UUID currentVersion = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);
    executeCql(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = '%s'"
                .formatted(keyspaceName));

    // when
    client.deploySchema(keyspaceName, currentVersion.toString(), true, SCHEMA_CONTENTS);

    // then
    ResultSet resultSet =
        executeCql(
                "select * from stargate_graphql.schema_source WHERE keyspace_name = '%s'"
                    .formatted(keyspaceName))
            .getResultSet();
    Row row = resultSet.getRows(0);
    assertThat(row).isNotNull();
    assertThat(Rows.getBoolean(row, "deployment_in_progress", resultSet.getColumnsList()))
        .isFalse();
  }

  @Test
  @DisplayName("Should fail to deploy schema when version doesn't match")
  public void deploySchemaWhenVersionMismatch() {
    // given
    UUID currentVersion = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);
    UUID wrongExpectedVersion = Uuids.timeBased();
    assertThat(wrongExpectedVersion).isNotEqualTo(currentVersion);

    // when
    String error =
        client.getDeploySchemaError(keyspaceName, wrongExpectedVersion.toString(), SCHEMA_CONTENTS);

    // then
    assertThat(error)
        .contains(
            String.format(
                "You specified expectedVersion %s, but there is a more recent version %s",
                wrongExpectedVersion, currentVersion));
  }

  @Test
  @DisplayName("Should fail to force deploy schema when version doesn't match")
  public void forceDeploySchemaWhenVersionMismatch() {
    // given

    UUID currentVersion = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);
    executeCql(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = '%s'"
                .formatted(keyspaceName));
    UUID wrongExpectedVersion = Uuids.timeBased();
    assertThat(wrongExpectedVersion).isNotEqualTo(currentVersion);

    // when
    String error =
        client.getDeploySchemaError(
            keyspaceName, wrongExpectedVersion.toString(), true, SCHEMA_CONTENTS);

    // then
    assertThat(error)
        .contains(
            String.format(
                "You specified expectedVersion %s, but there is a more recent version %s",
                wrongExpectedVersion, currentVersion));
  }

  @Test
  @DisplayName("Should fail to deploy schema when previous version expected but table is empty")
  public void deploySchemaWhenPreviousVersionExpectedButTableEmpty() {
    // when
    UUID wrongExpectedVersion = Uuids.timeBased();
    String error =
        client.getDeploySchemaError(keyspaceName, wrongExpectedVersion.toString(), SCHEMA_CONTENTS);

    // then
    assertThat(error).contains("You specified expectedVersion but no previous version was found");
  }

  @Test
  @DisplayName(
      "Should purge older schema entries, keeping only last SchemaSourceDao#NUMBER_OF_RETAINED_SCHEMA_VERSIONS versions")
  public void purgeOldSchemaEntriesOnInsert() {
    // given inserted NUMBER_OF_RETAINED_SCHEMA_VERSIONS + N schemas

    int numberOfSchemasAboveThreshold = 5;
    int numberOfVersionsToInsert =
        NUMBER_OF_RETAINED_SCHEMA_VERSIONS + numberOfSchemasAboveThreshold;

    // when deploying schemas
    List<UUID> schemasVersions = new ArrayList<>();
    UUID lastVersion = null;
    for (int i = 0; i < numberOfVersionsToInsert; i++) {
      lastVersion =
          client.deploySchema(
              keyspaceName, lastVersion == null ? null : lastVersion.toString(), SCHEMA_CONTENTS);
      schemasVersions.add(lastVersion);
    }

    // then the last NUMBER_OF_RETAINED_SCHEMA_VERSIONS schemas should be present
    List<UUID> removedVersions = schemasVersions.subList(0, numberOfSchemasAboveThreshold);
    List<UUID> presentVersions =
        schemasVersions.subList(numberOfSchemasAboveThreshold, schemasVersions.size());
    // all removed versions should return 404
    for (UUID version : removedVersions) {
      client.getSchemaFile(
          keyspaceName, version.toString(), Response.Status.NOT_FOUND.getStatusCode());
    }
    // rest of the schemas should be present
    for (UUID version : presentVersions) {
      assertThat(client.getSchemaFile(keyspaceName, version.toString())).isNotNull();
    }
  }

  @Test
  @DisplayName("Should undeploy schema")
  public void undeploySchema() {
    // given

    UUID version1 = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);
    UUID version2 = client.deploySchema(keyspaceName, version1.toString(), SCHEMA_CONTENTS);

    // when
    client.undeploySchema(keyspaceName, version2.toString());

    // then
    // rows are still here but none is marked as latest
    ResultSet resultSet =
        executeCql(
                "select * from stargate_graphql.schema_source where keyspace_name = '%s'"
                    .formatted(keyspaceName))
            .getResultSet();
    List<Row> rows = resultSet.getRowsList();
    assertThat(rows).hasSize(2);
    assertThat(Rows.getValue(rows.get(0), "latest_version", resultSet.getColumnsList()).hasNull())
        .isTrue();
  }

  @Test
  @DisplayName("Should redeploy schema after undeployment")
  public void undeployAndRedeploySchema() {
    // given

    UUID version1 = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);
    UUID version2 = client.deploySchema(keyspaceName, version1.toString(), SCHEMA_CONTENTS);
    client.undeploySchema(keyspaceName, version2.toString());

    // when
    // we don't require the previous version here, because it's considered inactive
    UUID version3 = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);

    // then
    ResultSet resultSet =
        executeCql(
                "select * from stargate_graphql.schema_source where keyspace_name = '%s'"
                    .formatted(keyspaceName))
            .getResultSet();
    List<Row> rows = resultSet.getRowsList();
    assertThat(rows).hasSize(3);
    assertThat(Rows.getUuid(rows.get(0), "latest_version", resultSet.getColumnsList()))
        .isEqualTo(version3);
  }

  @Test
  @DisplayName("Should fail to undeploy schema when version doesn't match")
  public void undeploySchemaWhenVersionMismatch() {
    // given

    UUID version = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);
    UUID wrongVersion = Uuids.timeBased();
    assertThat(wrongVersion).isNotEqualTo(version);

    // when
    String error = client.getUndeploySchemaError(keyspaceName, wrongVersion.toString());

    // then
    assertThat(error)
        .contains(
            String.format(
                "You specified expectedVersion %s, but there is a more recent version %s",
                wrongVersion, version));
  }

  @Test
  @DisplayName("Should fail to undeploy schema when current still in progress")
  public void undeploySchemaWhenInProgress() {
    // given
    UUID version = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);
    executeCql(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = '%s'"
                .formatted(keyspaceName));

    // when
    String error = client.getUndeploySchemaError(keyspaceName, version.toString());

    // then
    assertThat(error).contains("It looks like someone else is deploying a new schema");
  }

  @Test
  @DisplayName("Should force undeploy schema when current still in progress")
  public void forceUndeploySchemaWhenInProgress() {
    // given
    UUID version = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);
    executeCql(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = '%s'"
                .formatted(keyspaceName));

    // when
    client.undeploySchema(keyspaceName, version.toString(), true);
    ResultSet resultSet =
        executeCql(
                "SELECT latest_version, deployment_in_progress "
                    + "FROM stargate_graphql.schema_source "
                    + "WHERE keyspace_name = '%s'".formatted(keyspaceName))
            .getResultSet();
    Row row = resultSet.getRows(0);

    // then
    assertThat(Rows.getValue(row, "latest_version", resultSet.getColumnsList()).hasNull()).isTrue();
    assertThat(Rows.getBoolean(row, "deployment_in_progress", resultSet.getColumnsList()))
        .isFalse();
  }

  @Test
  @DisplayName("Should fail to force undeploy schema when version doesn't match")
  public void forceUndeploySchemaWhenVersionMismatch() {
    // given
    UUID version = client.deploySchema(keyspaceName, SCHEMA_CONTENTS);
    executeCql(
        "UPDATE stargate_graphql.schema_source "
            + "SET deployment_in_progress = true WHERE keyspace_name = '%s'"
                .formatted(keyspaceName));
    UUID wrongVersion = Uuids.timeBased();
    assertThat(wrongVersion).isNotEqualTo(version);

    // when
    String error = client.getUndeploySchemaError(keyspaceName, wrongVersion.toString());

    // then
    assertThat(error)
        .contains(
            String.format(
                "You specified expectedVersion %s, but there is a more recent version %s",
                wrongVersion, version));
  }

  @Test
  @DisplayName("Should not include stacktrace in error response")
  public void deploySchemaErrorNoStacktrace() {
    // given
    String invalidSchema = "type Foo { id ID }"; // missing colon before `ID`

    // when
    List<Map<String, Object>> errors =
        client.getDeploySchemaErrors(keyspaceName, null, invalidSchema);

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
  public void noDeployedSchema() {
    // When
    Object response =
        client.executeAdminQuery(
            String.format("{ schema(keyspace: \"%s\") { deployDate, contents} }", keyspaceName));

    // Then
    assertThat(JsonPath.<Object>read(response, "$.schema")).isNull();
  }
}