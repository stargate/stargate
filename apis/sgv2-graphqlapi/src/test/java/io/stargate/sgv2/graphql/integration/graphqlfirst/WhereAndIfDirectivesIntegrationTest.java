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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WhereAndIfDirectivesIntegrationTest extends GraphqlFirstIntegrationTest {

  @Test
  @DisplayName(
      "Should fail when deploying schema with a field annotated with both cql_if and cql_where")
  public void shouldFailToDeploySchemaWithAFieldAnnotatedWithBothCqlIfAndCqlWhere() {
    // given, when
    Map<String, Object> errors =
        client
            .getDeploySchemaErrors(
                keyspaceId.asInternal(),
                null,
                "type Foo @cql_input {\n"
                    + "  pk: Int! @cql_column(partitionKey: true)\n"
                    + "  v: Int\n"
                    + " "
                    + "}\n"
                    + "type DeleteFooResult @cql_payload {\n"
                    + "  applied: Boolean\n"
                    + "}\n"
                    + "type Query {\n"
                    + "  foo(pk: Int!): Foo\n"
                    + "}\n"
                    + "type DeleteFooResponse @cql_payload {\n"
                    + "  applied: Boolean"
                    + "}\n"
                    + "type Mutation {\n"
                    + "  deleteFooWhereAndIfOnTheSameField(\n"
                    + "pk: Int\n"
                    + "v: Int @cql_if(field: \"v\", predicate: GT) @cql_where(field: \"v\" predicate: GT)\n"
                    + "): Boolean\n"
                    + "    @cql_delete(targetEntity: \"Foo\")\n"
                    + "}")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains(
            "Operation deleteFooWhereAndIfOnTheSameField: argument v can only use one of @cql_where,@cql_if");
  }

  @Test
  @DisplayName("Should fail when deploying schema with a SELECT query field annotated with cql_if")
  public void shouldFailToDeploySchemaWithASelectQueryFieldAnnotatedWithCqlIf() {
    // given, when
    Map<String, Object> errors =
        client
            .getDeploySchemaErrors(
                keyspaceId.asInternal(),
                null,
                "type Foo @cql_input {\n"
                    + "  pk: Int! @cql_column(partitionKey: true)\n"
                    + "  v: Int\n"
                    + " "
                    + "}\n"
                    + "type DeleteFooResult @cql_payload {\n"
                    + "  applied: Boolean\n"
                    + "}\n"
                    + "type Query {\n"
                    + "  foo(pk: Int! @cql_if(field: \"v\", predicate: EQ)): Foo\n"
                    + "}\n")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains("Operation foo: @cql_if is not allowed on SELECT arguments (pk)");
  }
}
