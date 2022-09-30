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
package io.stargate.sgv2.graphql.integration.graphqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SelectIntegrationTest extends GraphqlFirstIntegrationTest {

  @BeforeAll
  public void deploySchema() {
    client.deploySchema(
        keyspaceId.asInternal(),
        "type Foo @cql_input {\n"
            + "  pk1: Int! @cql_column(partitionKey: true)\n"
            + "  pk2: Int! @cql_column(partitionKey: true)\n"
            + "  cc1: Int! @cql_column(clusteringOrder: ASC)\n"
            + "  cc2: Int! @cql_column(clusteringOrder: DESC)\n"
            + "  v: Int @cql_index\n"
            + "}\n"
            + "type SelectFooResult @cql_payload {\n"
            + "  data: [Foo]\n"
            + "  pagingState: String\n"
            + "}\n"
            + "type Query {\n"
            + "  foo(pk1: Int!, pk2: Int!, cc1: Int, cc2: Int): Foo\n"
            + "  fooByPkAndCc1(pk1: Int!, pk2: Int!, cc1: Int!): [Foo]\n"
            + "  fooByPk(pk1: Int!, pk2: Int): [Foo]\n"
            + "  fooByPkLimit(\n"
            + "    pk1: Int!,\n"
            + "    pk2: Int!\n"
            + "  ): [Foo] @cql_select(limit: 5)\n"
            + "  fooByPkPaginated(\n"
            + "    pk1: Int!,\n"
            + "    pk2: Int!,\n"
            + "    pagingState: String @cql_pagingState\n"
            + "  ): SelectFooResult @cql_select(pageSize: 5)\n"
            + "  fooByV(v: Int): [Foo]\n"
            + "  fooByPkAndV(pk1: Int, pk2: Int, v: Int): [Foo]\n"
            + "}\n"
            + "type Mutation {\n"
            + "  insertFoo(foo: FooInput!): Foo \n"
            + "}");

    insert(1, 2, 1, 2, 2);
    insert(1, 2, 1, 1, 1);
    insert(1, 2, 2, 2, 2);
    insert(1, 2, 2, 1, 1);
    insert(1, 2, 3, 2, 2);
    insert(1, 2, 3, 1, 1);
    insert(1, 2, 4, 2, 2);
    insert(1, 2, 4, 1, 1);
  }

  private void insert(int pk1, int pk2, int cc1, int cc2, int v) {
    client.executeKeyspaceQuery(
        keyspaceId.asInternal(),
        String.format(
            "mutation {\n"
                + "  result: insertFoo(foo: {pk1: %d, pk2: %d, cc1: %d, cc2: %d, v: %d}) {\n"
                + "    pk1, pk2, cc1, cc2\n"
                + "  }\n"
                + "}",
            pk1, pk2, cc1, cc2, v));
  }

  @Test
  @DisplayName("Should select single row by full primary key")
  public void selectFullPrimaryKey() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "query {\n"
                + "  result: foo(pk1: 1, pk2: 2, cc1: 1, cc2: 1) {\n"
                + "    pk1,pk2,cc1,cc2\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Integer>read(response, "$.result.pk1")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.result.pk2")).isEqualTo(2);
    assertThat(JsonPath.<Integer>read(response, "$.result.cc1")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.result.cc2")).isEqualTo(1);
  }

  @Test
  @DisplayName("Should select by primary key prefix")
  public void selectPrimaryKeyPrefix() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "query {\n"
                + "  results: fooByPkAndCc1(pk1: 1, pk2: 2, cc1: 1) {\n"
                + "    pk1,pk2,cc1,cc2\n"
                + "  }\n"
                + "}");

    // then
    assertResults(
        JsonPath.read(response, "$.results"), new int[] {1, 2, 1, 2}, new int[] {1, 2, 1, 1});
  }

  @Test
  @DisplayName("Should select full partition")
  public void selectFullPartition() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "query {\n"
                + "  results: fooByPk(pk1: 1, pk2: 2) {\n"
                + "    pk1,pk2,cc1,cc2\n"
                + "  }\n"
                + "}");

    // then
    assertResults(
        JsonPath.read(response, "$.results"),
        new int[] {1, 2, 1, 2},
        new int[] {1, 2, 1, 1},
        new int[] {1, 2, 2, 2},
        new int[] {1, 2, 2, 1},
        new int[] {1, 2, 3, 2},
        new int[] {1, 2, 3, 1},
        new int[] {1, 2, 4, 2},
        new int[] {1, 2, 4, 1});
  }

  @Test
  @DisplayName("Should select full partition with limit")
  public void selectFullPartitionWithLimit() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "query {\n"
                + "  results: fooByPkLimit(pk1: 1, pk2: 2) {\n"
                + "    pk1,pk2,cc1,cc2\n"
                + "  }\n"
                + "}");

    // then
    assertResults(
        JsonPath.read(response, "$.results"),
        new int[] {1, 2, 1, 2},
        new int[] {1, 2, 1, 1},
        new int[] {1, 2, 2, 2},
        new int[] {1, 2, 2, 1},
        new int[] {1, 2, 3, 2});
  }

  @Test
  @DisplayName("Should select full partition with pagination")
  public void selectFullPartitionWithPagination() {
    Object page1 =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "query {\n"
                + "  results: fooByPkPaginated(pk1: 1, pk2: 2) {\n"
                + "    data { pk1, pk2, cc1, cc2 }\n"
                + "    pagingState \n"
                + "  }\n"
                + "}");

    assertResults(
        JsonPath.read(page1, "$.results.data"),
        new int[] {1, 2, 1, 2},
        new int[] {1, 2, 1, 1},
        new int[] {1, 2, 2, 2},
        new int[] {1, 2, 2, 1},
        new int[] {1, 2, 3, 2});
    String pagingState = JsonPath.read(page1, "$.results.pagingState");
    assertThat(pagingState).isNotNull();

    Object page2 =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            String.format(
                "query {\n"
                    + "  results: fooByPkPaginated(pk1: 1, pk2: 2, pagingState: \"%s\") {\n"
                    + "    data { pk1, pk2, cc1, cc2 }\n"
                    + "    pagingState \n"
                    + "  }\n"
                    + "}",
                pagingState));

    assertResults(
        JsonPath.read(page2, "$.results.data"),
        new int[] {1, 2, 3, 1},
        new int[] {1, 2, 4, 2},
        new int[] {1, 2, 4, 1});
    pagingState = JsonPath.read(page2, "$.results.pagingState");
    assertThat(pagingState).isNull();
  }

  @Test
  @DisplayName("Should fail if not all partition keys are present")
  public void selectWithMissingPartitionKey() {
    assertThat(client.getKeyspaceError(keyspaceId.asInternal(), "{ fooByPk(pk1: 1) { pk1 } }"))
        .contains(
            "Invalid arguments: every partition key field of type Foo must be present "
                + "(expected: pk1, pk2)");
  }

  @Test
  @DisplayName("Should fail if partial primary key is not a prefix")
  public void selectByPartialPrimaryKeyNotPrefix() {
    assertThat(
            client.getKeyspaceError(
                keyspaceId.asInternal(), "{ foo(pk1: 1, pk2: 2, cc2: 1) { pk1 } }"))
        .contains(
            "Invalid arguments: clustering field cc1 is not restricted by EQ or IN, "
                + "so no other clustering field after it can be restricted (offending: cc2).");
  }

  @Test
  @DisplayName("Should select by indexed column")
  public void selectByIndex() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "query { results: fooByV(v: 1) { pk1,pk2,cc1,cc2 } }");

    // then
    assertResults(
        JsonPath.read(response, "$.results"),
        new int[] {1, 2, 1, 1},
        new int[] {1, 2, 2, 1},
        new int[] {1, 2, 3, 1},
        new int[] {1, 2, 4, 1});
  }

  @Test
  @DisplayName("Should select by partition key and indexed column")
  public void selectByPartitionKeyAndIndex() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "query { results: fooByPkAndV(pk1: 1, pk2: 2, v: 1) { pk1,pk2,cc1,cc2 } }");

    // then
    assertResults(
        JsonPath.read(response, "$.results"),
        new int[] {1, 2, 1, 1},
        new int[] {1, 2, 2, 1},
        new int[] {1, 2, 3, 1},
        new int[] {1, 2, 4, 1});
  }

  @Test
  @DisplayName("Should fail with partial partition key and index")
  public void selectByPartialPartitionKeyAndIndex() {
    assertThat(
            client.getKeyspaceError(
                keyspaceId.asInternal(), "{ fooByPkAndV(pk1: 1, v: 1) { pk1 } }"))
        .contains(
            "Invalid arguments: when an indexed field is present, either none or all "
                + "of the partition key fields must be present (expected pk1, pk2).");
  }

  private void assertResults(Object response, int[]... rows) {
    assertThat(JsonPath.<Integer>read(response, "$.length()")).isEqualTo(rows.length);
    for (int i = 0; i < rows.length; i++) {
      int[] row = rows[i];
      assertThat(JsonPath.<Integer>read(response, "$[" + i + "].pk1")).isEqualTo(row[0]);
      assertThat(JsonPath.<Integer>read(response, "$[" + i + "].pk2")).isEqualTo(row[1]);
      assertThat(JsonPath.<Integer>read(response, "$[" + i + "].cc1")).isEqualTo(row[2]);
      assertThat(JsonPath.<Integer>read(response, "$[" + i + "].cc2")).isEqualTo(row[3]);
    }
  }
}
