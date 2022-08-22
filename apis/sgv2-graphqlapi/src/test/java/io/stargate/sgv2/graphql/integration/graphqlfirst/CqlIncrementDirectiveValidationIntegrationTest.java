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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.util.Map;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CqlIncrementDirectiveValidationIntegrationTest extends GraphqlFirstIntegrationTest {

  @Test
  @DisplayName("Should fail when deploying schema with a pk field annotated with cql_increment")
  public void shouldFailToDeploySchemaWithAPKFieldAnnotatedWithCqlIncrement() {
    // given, when
    Map<String, Object> errors =
        client
            .getDeploySchemaErrors(
                keyspaceId.asInternal(),
                null,
                "type Counters @cql_input {\n"
                    + "  k: Int! @cql_column(partitionKey: true)\n"
                    + "  c: Counter\n"
                    + "}\n"
                    + "type Query { counters(k: Int!): Counters }\n"
                    + "type Mutation {\n"
                    + " updateCountersIncrement(\n"
                    + "    k: Int @cql_increment\n"
                    + "    cInc: Int\n"
                    + "  ): Boolean\n"
                    + "@cql_update(targetEntity: \"Counters\")\n"
                    + "}")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains(
            "Operation updateCountersIncrement: @cql_increment is not allowed on UPDATE primary key arguments (k)");
  }

  @Test
  @DisplayName(
      "Should fail when deploying schema with a cql_increment prepend=true on non-collection field")
  public void shouldFailToDeploySchemaWithACqlIncrementPrependTrueOnANonCollectionField() {
    // given, when
    Map<String, Object> errors =
        client
            .getDeploySchemaErrors(
                keyspaceId.asInternal(),
                null,
                "type Counters @cql_input {\n"
                    + "  k: Int! @cql_column(partitionKey: true)\n"
                    + "  c: Counter\n"
                    + "}\n"
                    + "type Query { counters(k: Int!): Counters }\n"
                    + "type Mutation {\n"
                    + " updateCountersIncrement(\n"
                    + "    k: Int \n"
                    + "    cInc: Int @cql_increment(field: \"c\", prepend: true)\n"
                    + "  ): Boolean\n"
                    + "@cql_update(targetEntity: \"Counters\")\n"
                    + "}")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains(
            "Operation updateCountersIncrement: @cql_increment.prepend can only be applied to list fields");
  }

  @Test
  @DisplayName("Should fail when deploying schema with cql_increment on a query field.")
  public void shouldFailToDeploySchemaWithIncrementOnQueryField() {
    // given, when
    Map<String, Object> errors =
        client
            .getDeploySchemaErrors(
                keyspaceId.asInternal(),
                null,
                "type Counters @cql_input {\n"
                    + "  k: Int! @cql_column(partitionKey: true)\n"
                    + "  c: Counter\n"
                    + "}\n"
                    + "type Query { counters(k: Int! @cql_increment): Counters }\n"
                    + "type Mutation {\n"
                    + " updateCountersIncrement(\n"
                    + "    k: Int\n"
                    + "    cInc: Int\n"
                    + "  ): Boolean\n"
                    + "@cql_update(targetEntity: \"Counters\")\n"
                    + "}")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains("Operation counters: @cql_increment is not allowed on SELECT arguments (k)");
  }

  @Test
  @DisplayName("Should fail when deploying schema with cql_increment on a delete field.")
  public void shouldFailToDeploySchemaWithIncrementOnDeleteField() {
    // given, when
    Map<String, Object> errors =
        client
            .getDeploySchemaErrors(
                keyspaceId.asInternal(),
                null,
                "type Counters @cql_input {\n"
                    + "  k: Int! @cql_column(partitionKey: true)\n"
                    + "  c: Counter\n"
                    + "}\n"
                    + "type Query { counters(k: Int!): Counters }\n"
                    + "type Mutation {\n"
                    + " delete(\n"
                    + "    k: Int @cql_increment\n"
                    + "  ): Boolean\n"
                    + "@cql_delete(targetEntity: \"Counters\")\n"
                    + "}")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains("Operation delete: @cql_increment is not allowed on DELETE arguments (k)");
  }

  @Test
  @DisplayName("Should fail when deploying schema with cql_increment on non Int or BigInt field.")
  public void shouldFailToDeploySchemaWithIncrementOnNonIntField() {
    // given, when
    Map<String, Object> errors =
        client
            .getDeploySchemaErrors(
                keyspaceId.asInternal(),
                null,
                "type Counters @cql_input {\n"
                    + "  k: Int! @cql_column(partitionKey: true)\n"
                    + "  c: Counter\n"
                    + "}\n"
                    + "type Query { counters(k: Int!): Counters }\n"
                    + "type Mutation {\n"
                    + " update(\n"
                    + "    k: Int\n"
                    + "    cInc: String @cql_increment(field: \"c\")\n"
                    + "  ): Boolean\n"
                    + "@cql_update(targetEntity: \"Counters\")\n"
                    + "}")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains(
            "Operation update: expected argument cInc to have a valid counter increment type (one of: Int, BigInt, Counter)");
  }

  @Test
  @DisplayName("Should fail when deploying schema with cql_increment on non Int or BigInt field.")
  public void shouldFailToDeploySchemaWithIncrementOnNotCounterField() {
    // given, when
    Map<String, Object> errors =
        client
            .getDeploySchemaErrors(
                keyspaceId.asInternal(),
                null,
                "type Counters @cql_input {\n"
                    + "  k: Int! @cql_column(partitionKey: true)\n"
                    + "  c: Int\n"
                    + "}\n"
                    + "type Query { counters(k: Int!): Counters }\n"
                    + "type Mutation {\n"
                    + " update(\n"
                    + "    k: Int\n"
                    + "    cInc: String @cql_increment(field: \"c\")\n"
                    + "  ): Boolean\n"
                    + "@cql_update(targetEntity: \"Counters\")\n"
                    + "}")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains(
            "Operation update: @cql_increment can only be applied to counter or collection fields");
  }
}
