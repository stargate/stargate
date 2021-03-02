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
package io.stargate.it.http.graphql.graphfirst;

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
            + "type Query {\n"
            + "  foo(pk1: Int!, pk2: Int!, cc1: Int!, cc2: Int!): Foo\n"
            + "  fooPartial(pk1: Int!, pk2: Int!, cc1: Int!): [Foo]\n"
            + "}\n"
            + "type Mutation {\n"
            + "  insertFoo(foo: FooInput!): Foo \n"
            + "}");

    CLIENT.executeNamespaceQuery(
        NAMESPACE,
        "mutation {\n"
            + "  result: insertFoo(foo: {pk1: 1, pk2: 2, cc1: 11, cc2: 12}) {\n"
            + "    pk1, pk2, cc1, cc2\n"
            + "  }\n"
            + "}");

    CLIENT.executeNamespaceQuery(
        NAMESPACE,
        "mutation {\n"
            + "  result: insertFoo(foo: {pk1: 1, pk2: 2, cc1: 11, cc2: 22}) {\n"
            + "    pk1, pk2, cc1, cc2\n"
            + "  }\n"
            + "}");
  }

  @Test
  @DisplayName("Should select an entity using all primary keys")
  public void shouldSelectFooUsingAllPrimaryKeys() {
    // when
    Object response =
        CLIENT.executeNamespaceQuery(
            NAMESPACE,
            "query {\n"
                + "  result: foo(pk1: 1, pk2: 2, cc1: 11, cc2: 12) {\n"
                + "    pk1,pk2,cc1,cc2\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.read(response, "$.result.pk1").toString()).isEqualTo("1");
    assertThat(JsonPath.read(response, "$.result.pk2").toString()).isEqualTo("2");
    assertThat(JsonPath.read(response, "$.result.cc1").toString()).isEqualTo("11");
    assertThat(JsonPath.read(response, "$.result.cc2").toString()).isEqualTo("12");
  }

  @Test
  @DisplayName("Should select a list of entities using non-all primary keys")
  public void shouldSelectListOfFooUsingNonAllPrimaryKeys() {
    // when
    Object response =
        CLIENT.executeNamespaceQuery(
            NAMESPACE,
            "query {\n"
                + "  result: fooPartial(pk1: 1, pk2: 2, cc1: 11) {\n"
                + "    pk1,pk2,cc1,cc2\n"
                + "  }\n"
                + "}");

    // then
    assertThat((Integer) JsonPath.read(response, "$.result.length()")).isEqualTo(2);
    assertThat(JsonPath.read(response, "$.result[0].pk1").toString()).isEqualTo("1");
    assertThat(JsonPath.read(response, "$.result[0].pk2").toString()).isEqualTo("2");
    assertThat(JsonPath.read(response, "$.result[0].cc1").toString()).isEqualTo("11");
    assertThat(JsonPath.read(response, "$.result[0].cc2").toString()).isEqualTo("22");
    assertThat(JsonPath.read(response, "$.result[1].pk1").toString()).isEqualTo("1");
    assertThat(JsonPath.read(response, "$.result[1].pk2").toString()).isEqualTo("2");
    assertThat(JsonPath.read(response, "$.result[1].cc1").toString()).isEqualTo("11");
    assertThat(JsonPath.read(response, "$.result[1].cc2").toString()).isEqualTo("12");
  }
}
