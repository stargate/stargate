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
public class BulkInsertValidationIntegrationTest extends GraphqlFirstIntegrationTest {

  @Test
  @DisplayName("Should fail when deploying schema with a bulk insert returning non-list.")
  public void shouldFailToDeploySchemaWithABulkInsertReturningNonList() {
    // given, when
    Map<String, Object> errors =
        client
            .getDeploySchemaErrors(
                keyspaceId.asInternal(),
                null,
                "type User @cql_input {\n"
                    + "  id: ID!\n"
                    + "  name: String\n"
                    + "  username: String\n"
                    + "}\n"
                    + "type Query { user(id: ID!): User }\n"
                    + "type InsertUserResponse @cql_payload {\n"
                    + "  applied: Boolean!\n"
                    + "  user: User!\n"
                    + "}\n"
                    + "type Mutation {\n"
                    + "  bulkInsertUsers(users: [UserInput!]): User\n"
                    + "}")
            .get(0);

    // then
    assertThat(getMappingErrors(errors))
        .contains(
            "Mutation bulkInsertUsers: invalid return type. For bulk inserts, expected list of User. ");
  }
}
