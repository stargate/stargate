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
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.util.List;
import java.util.Map;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SelectCustomConditionsIntegrationTest extends GraphqlFirstIntegrationTest {

  @BeforeAll
  public void deploySchema() {
    client.deploySchema(
        keyspaceId.asInternal(),
        "type Foo @cql_input {\n"
            + "  pk: Int! @cql_column(partitionKey: true)\n"
            + "  cc1: Int! @cql_column(clusteringOrder: ASC)\n"
            + "  cc2: Int! @cql_column(clusteringOrder: ASC)\n"
            + "}\n"
            + "type Query {\n"
            + "  foosByPks(\n"
            + "    pks: [Int] @cql_where(field: \"pk\", predicate: IN)\n"
            + "  ): [Foo]\n"
            + "  foosByCc1Range(\n"
            + "    pk: Int,"
            + "    minCc1: Int @cql_where(field: \"cc1\", predicate: GTE)\n"
            + "    maxCc1: Int @cql_where(field: \"cc1\", predicate: LTE)\n"
            + "  ): [Foo]\n"
            + "  foosByCc1sAndCc2(\n"
            + "    pk: Int,"
            + "    cc1s: [Int] @cql_where(field: \"cc1\", predicate: IN)\n"
            + "    cc2: Int\n"
            + "  ): [Foo]\n"
            + "}\n"
            + "type Mutation {\n"
            + "  insertFoo(foo: FooInput!): Foo \n"
            + "}");

    insert(1, 1, 1);
    insert(1, 2, 2);
    insert(1, 3, 3);
    insert(1, 4, 4);

    insert(2, 1, 1);
    insert(2, 2, 1);
    insert(2, 3, 2);
    insert(2, 4, 1);

    insert(3, 1, 1);
    insert(3, 2, 2);
    insert(3, 3, 3);
    insert(3, 4, 4);
  }

  private void insert(int pk, int cc1, int cc2) {
    client.executeKeyspaceQuery(
        keyspaceId.asInternal(),
        String.format(
            "mutation {\n"
                + "  result: insertFoo(foo: {pk: %d, cc1: %d, cc2: %d}) {\n"
                + "    pk, cc1, cc2\n"
                + "  }\n"
                + "}",
            pk, cc1, cc2));
  }

  @Test
  @DisplayName("Should select multiple partitions with IN predicate")
  public void selectMultiplePartitions() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "{\n" + "  results: foosByPks(pks: [1, 3]) {\n" + "    pk, cc1, cc2\n" + "  }\n" + "}");

    // then
    List<Map<String, Object>> results = JsonPath.read(response, "$.results");
    assertThat(results).hasSize(8);
    for (Map<String, Object> result : results) {
      assertThat(result.get("pk")).isIn(1, 3);
    }
  }

  @Test
  @DisplayName("Should select clustering key range")
  public void selectClusteringKeyRange() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "{\n"
                + "  results: foosByCc1Range(pk: 1, minCc1: 2, maxCc1: 3) {\n"
                + "    pk, cc1, cc2\n"
                + "  }\n"
                + "}");

    // then
    assertResults(JsonPath.read(response, "$.results"), new int[] {1, 2, 2}, new int[] {1, 3, 3});

    // Also works with an open range:
    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "{\n"
                + "  results: foosByCc1Range(pk: 1, minCc1: 3) {\n"
                + "    pk, cc1, cc2\n"
                + "  }\n"
                + "}");

    // then
    assertResults(JsonPath.read(response, "$.results"), new int[] {1, 3, 3}, new int[] {1, 4, 4});
  }

  @Test
  @DisplayName("Should select by clustering key if previous restricted with IN")
  public void selectByMultipleClusteringKeysWithInPredicate() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "{\n"
                + "  results: foosByCc1sAndCc2(pk: 2, cc1s: [2, 3, 4], cc2: 1) {\n"
                + "    pk, cc1, cc2\n"
                + "  }\n"
                + "}");

    // then
    assertResults(JsonPath.read(response, "$.results"), new int[] {2, 2, 1}, new int[] {2, 4, 1});
  }

  @Test
  @DisplayName("Should fail if restricted clustering keys are not a prefix")
  public void selectWithInvalidClusteringKeyCombination() {
    assertThat(
            client.getKeyspaceError(
                keyspaceId.asInternal(),
                "{\n"
                    + "  results: foosByCc1sAndCc2(pk: 2, cc2: 1) {\n"
                    + "    pk, cc1, cc2\n"
                    + "  }\n"
                    + "}"))
        .contains(
            "Invalid arguments: clustering field cc1 is not restricted by EQ or IN, "
                + "so no other clustering field after it can be restricted (offending: cc2).");
  }

  private void assertResults(Object response, int[]... rows) {
    assertThat(JsonPath.<Integer>read(response, "$.length()")).isEqualTo(rows.length);
    for (int i = 0; i < rows.length; i++) {
      int[] row = rows[i];
      assertThat(JsonPath.<Integer>read(response, "$[" + i + "].pk")).isEqualTo(row[0]);
      assertThat(JsonPath.<Integer>read(response, "$[" + i + "].cc1")).isEqualTo(row[1]);
      assertThat(JsonPath.<Integer>read(response, "$[" + i + "].cc2")).isEqualTo(row[2]);
    }
  }
}
