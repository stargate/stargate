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
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class WhereAndIfDirectivesTest extends GraphqlFirstTestBase {
  private static GraphqlFirstClient CLIENT;
  private static String KEYSPACE;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceId) {
    CLIENT =
        new GraphqlFirstClient(
            cluster.seedAddress(), RestUtils.getAuthToken(cluster.seedAddress()));
    KEYSPACE = keyspaceId.asInternal();
  }

  @Test
  @DisplayName(
      "Should fail when deploying schema with a field annotated with both cql_if and cql_where")
  public void shouldFailToDeploySchemaWithAFieldAnnotatedWithBothCqlIfAndCqlWhere() {
    // given, when
    Map<String, Object> errors =
        CLIENT.getDeploySchemaErrors(
            KEYSPACE,
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
                + "}");

    // then
    assertThat(getMappingErrors(errors))
        .contains("You cannot set both: cql_if and cql_where directives on the same field: v");
  }

  @Test
  @DisplayName("Should fail when deploying schema with a SELECT query field annotated with cql_if")
  public void shouldFailToDeploySchemaWithASelectQueryFieldAnnotatedWithCqlIf() {
    // given, when
    Map<String, Object> errors =
        CLIENT.getDeploySchemaErrors(
            KEYSPACE,
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
                + "}\n");

    // then
    assertThat(getMappingErrors(errors))
        .contains("@cql_if is not supported for select query, but it was set on one of the fields");
  }

  @SuppressWarnings("unchecked")
  private String getMappingErrors(Map<String, Object> errors) {
    Map<String, Object> value =
        ((Map<String, List<Map<String, Object>>>) errors.get("extensions"))
            .get("mappingErrors")
            .get(0);
    return (String) value.get("message");
  }
}
