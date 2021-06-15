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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class TypeCheckValidationTest extends GraphqlFirstTestBase {
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
  @DisplayName("Should fail when deploying schema with a list inner-type not matching.")
  public void shouldFailToDeploySchemaWithAListInnerTypeNotMatching() {
    Map<String, Object> errors =
        CLIENT
            .getDeploySchemaErrors(
                KEYSPACE,
                null,
                "type User @cql_input {\n"
                    + "  pk: Int! @cql_column(partitionKey: true)\n"
                    + "  v: [Int]\n"
                    + "}\n"
                    + "type Query { user(pk: Int!): User }\n"
                    + "type UpdateUserResponse @cql_payload {\n"
                    + "  applied: Boolean\n"
                    + "  user: User!\n"
                    + "}\n"
                    + "type Mutation {\n"
                    + "  updateUser(\n"
                    + ""
                    + "pk: Int\n"
                    + "v: [Boolean] @cql_if(field: \"v\", predicate: IN)\n"
                    + "): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
                    + "}")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains(
            "Operation updateUser: expected argument v to have a list of [Int] type to match User.v");
  }

  @Test
  @DisplayName("Should fail when deploying schema with a non-list field whereas it expects a list.")
  public void shouldFailToDeploySchemaWithANonListFieldWhereasItExpectsAList() {
    Map<String, Object> errors =
        CLIENT
            .getDeploySchemaErrors(
                KEYSPACE,
                null,
                "type User @cql_input {\n"
                    + "  pk: Int! @cql_column(partitionKey: true)\n"
                    + "  v: [Int]\n"
                    + "}\n"
                    + "type Query { user(pk: Int!): User }\n"
                    + "type UpdateUserResponse @cql_payload {\n"
                    + "  applied: Boolean\n"
                    + "  user: User!\n"
                    + "}\n"
                    + "type Mutation {\n"
                    + "  updateUser(\n"
                    + ""
                    + "pk: Int\n"
                    + "v: Int @cql_if(field: \"v\", predicate: IN)\n"
                    + "): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
                    + "}")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains(
            "Operation updateUser: expected argument v to have a list of [Int] type to match User.v");
  }
}
