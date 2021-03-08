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
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class DeleteTest extends GraphqlFirstTestBase {
  private static GraphqlFirstClient CLIENT;
  private static String NAMESPACE;
  private static CqlSession SESSION;

  @BeforeAll
  public static void setup(
      StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    CLIENT =
        new GraphqlFirstClient(
            cluster.seedAddress(), RestUtils.getAuthToken(cluster.seedAddress()));
    NAMESPACE = keyspaceId.asInternal();
    SESSION = session;
    CLIENT.deploySchema(
        NAMESPACE,
        "type Foo @cql_input {\n"
            + "  pk: Int! @cql_column(partitionKey: true)\n"
            + "  cc: Int! @cql_column(clusteringOrder: ASC)\n"
            + "}\n"
            + "type DeleteFooResult @cql_payload {\n"
            + "  applied: Boolean\n"
            + "}\n"
            + "type Query {\n"
            + "  foo(pk: Int!, cc: Int!): Foo\n"
            + "}\n"
            + "type DeleteFooResponse @cql_payload {\n"
            + "  applied: Boolean"
            + "}\n"
            + "type Mutation {\n"
            + "  insertFoo(foo: FooInput!): Foo \n"
            + "  deleteFoo(foo: FooInput!): Boolean \n"
            + "  deleteFooIfExists(foo: FooInput!): DeleteFooResponse \n"
            + "}");
  }

  @BeforeEach
  public void cleanupData() {
    SESSION.execute("truncate table \"Foo\"");
  }

  private static void insert(int pk, int cc) {
    SESSION.execute("INSERT INTO \"Foo\"(pk, cc) VALUES (?, ?)", pk, cc);
  }

  private static boolean exists(int pk, int cc) {
    ResultSet resultSet = SESSION.execute("SELECT * FROM \"Foo\" WHERE pk = ? AND cc = ?", pk, cc);
    return resultSet.one() != null;
  }

  @Test
  @DisplayName("Should delete single row by full primary key")
  public void deleteByFullPrimaryKey() {
    // Given
    insert(1, 1);

    // when
    Object response =
        CLIENT.executeNamespaceQuery(NAMESPACE, "mutation { deleteFoo(foo: {pk: 1, cc: 1}) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFoo")).isTrue();
    assertThat(exists(1, 1)).isFalse();

    // Deleting a non-existing row always returns true:
    // when
    response =
        CLIENT.executeNamespaceQuery(NAMESPACE, "mutation { deleteFoo(foo: {pk: 1, cc: 1}) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFoo")).isTrue();
  }

  @Test
  @DisplayName("Should delete single row by full primary key if exists")
  public void deleteByFullPrimaryKeyIfExists() {
    // Given
    insert(1, 1);

    // when
    Object response =
        CLIENT.executeNamespaceQuery(
            NAMESPACE, "mutation { deleteFooIfExists(foo: {pk: 1, cc: 1}) {applied} }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooIfExists.applied")).isTrue();
    assertThat(exists(1, 1)).isFalse();

    // Deleting a non-existing row returns false:
    // when
    response =
        CLIENT.executeNamespaceQuery(
            NAMESPACE, "mutation { deleteFooIfExists(foo: {pk: 1, cc: 1}) {applied} }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooIfExists.applied")).isFalse();
    assertThat(exists(1, 1)).isFalse();
  }
}
