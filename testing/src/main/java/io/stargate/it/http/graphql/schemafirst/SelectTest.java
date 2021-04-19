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
package io.stargate.it.http.graphql.schemafirst;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class SelectTest extends GraphqlFirstTestBase {
  private static GraphqlFirstClient CLIENT;
  private static String NAMESPACE;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceId) {
    CLIENT =
        new GraphqlFirstClient(
            cluster.seedAddress(), RestUtils.getAuthToken(cluster.seedAddress()));
    NAMESPACE = keyspaceId.asInternal();
    CLIENT.deploySchema(
        NAMESPACE,
        "type Foo @cql_input {\n"
            + "  pk1: Int! @cql_column(partitionKey: true)\n"
            + "  pk2: Int! @cql_column(partitionKey: true)\n"
            + "  cc1: Int! @cql_column(clusteringOrder: ASC)\n"
            + "  cc2: Int! @cql_column(clusteringOrder: DESC)\n"
            + "}\n"
            + "type SelectFooResult @cql_payload {\n"
            + "  data: [Foo]\n"
            + "  pagingState: String\n"
            + "}\n"
            + "type Query {\n"
            + "  foo(pk1: Int!, pk2: Int!, cc1: Int!, cc2: Int!): Foo\n"
            + "  fooByPkAndCc1(pk1: Int!, pk2: Int!, cc1: Int!): [Foo]\n"
            + "  fooByPk(pk1: Int!, pk2: Int!): [Foo]\n"
            + "  fooByPkLimit(\n"
            + "    pk1: Int!,\n"
            + "    pk2: Int!\n"
            + "  ): [Foo] @cql_select(limit: 5)\n"
            + "  fooByPkPaginated(\n"
            + "    pk1: Int!,\n"
            + "    pk2: Int!,\n"
            + "    pagingState: String @cql_pagingState\n"
            + "  ): SelectFooResult @cql_select(pageSize: 5)\n"
            + "}\n"
            + "type Mutation {\n"
            + "  insertFoo(foo: FooInput!): Foo \n"
            + "}");

    insert(1, 2, 1, 2);
    insert(1, 2, 1, 1);
    insert(1, 2, 2, 2);
    insert(1, 2, 2, 1);
    insert(1, 2, 3, 2);
    insert(1, 2, 3, 1);
    insert(1, 2, 4, 2);
    insert(1, 2, 4, 1);
  }

  private static void insert(int pk1, int pk2, int cc1, int cc2) {
    CLIENT.executeNamespaceQuery(
        NAMESPACE,
        String.format(
            "mutation {\n"
                + "  result: insertFoo(foo: {pk1: %d, pk2: %d, cc1: %d, cc2: %d}) {\n"
                + "    pk1, pk2, cc1, cc2\n"
                + "  }\n"
                + "}",
            pk1, pk2, cc1, cc2));
  }

  @Test
  @DisplayName("Should select single row by full primary key")
  public void selectFullPrimaryKey() {
    // when
    Object response =
        CLIENT.executeNamespaceQuery(
            NAMESPACE,
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
        CLIENT.executeNamespaceQuery(
            NAMESPACE,
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
        CLIENT.executeNamespaceQuery(
            NAMESPACE,
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
        CLIENT.executeNamespaceQuery(
            NAMESPACE,
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
        CLIENT.executeNamespaceQuery(
            NAMESPACE,
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
        CLIENT.executeNamespaceQuery(
            NAMESPACE,
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
