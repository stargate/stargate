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
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.ApiServiceConnectionInfo;
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
  private static String KEYSPACE;
  private static CqlSession SESSION;

  @BeforeAll
  public static void setup(
      StargateConnectionInfo stargateBackend,
      ApiServiceConnectionInfo stargateGraphqlApi,
      @TestKeyspace CqlIdentifier keyspaceId,
      CqlSession session) {
    CLIENT =
        new GraphqlFirstClient(
            stargateGraphqlApi.host(),
            stargateGraphqlApi.port(),
            RestUtils.getAuthToken(stargateBackend.seedAddress()));
    KEYSPACE = keyspaceId.asInternal();
    SESSION = session;
    CLIENT.deploySchema(
        KEYSPACE,
        "type Foo @cql_input {\n"
            + "  pk: Int! @cql_column(partitionKey: true)\n"
            + "  cc1: Int! @cql_column(clusteringOrder: ASC)\n"
            + "  cc2: Int! @cql_column(clusteringOrder: ASC)\n"
            + "}\n"
            + "type DeleteFooResult @cql_payload {\n"
            + "  applied: Boolean\n"
            + "}\n"
            + "type Query {\n"
            + "  foo(pk: Int!, cc1: Int!, cc2: Int!): Foo\n"
            + "}\n"
            + "type DeleteFooResponse @cql_payload {\n"
            + "  applied: Boolean"
            + "}\n"
            + "type Mutation {\n"
            + "  deleteFoo(foo: FooInput!): Boolean \n"
            + "  deleteFoo2(pk: Int, cc1: Int, cc2: Int): Boolean\n"
            + "    @cql_delete(targetEntity: \"Foo\")\n"
            + "  deleteFooIfExists(foo: FooInput!): DeleteFooResponse \n"
            + "  deleteFooPartition(pk: Int): Boolean\n"
            + "    @cql_delete(targetEntity: \"Foo\")\n"
            + "}");
  }

  @BeforeEach
  public void cleanupData() {
    SESSION.execute("truncate table \"Foo\"");
  }

  private static void insert(int pk, int cc1, int cc2) {
    SESSION.execute("INSERT INTO \"Foo\"(pk, cc1, cc2) VALUES (?, ?, ?)", pk, cc1, cc2);
  }

  private static boolean exists(int pk, int cc1, int cc2) {
    ResultSet resultSet =
        SESSION.execute("SELECT * FROM \"Foo\" WHERE pk = ? AND cc1 = ? AND cc2 = ?", pk, cc1, cc2);
    return resultSet.one() != null;
  }

  @Test
  @DisplayName("Should delete single row by full primary key")
  public void deleteByFullPrimaryKey() {
    // Given
    insert(1, 1, 1);

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE, "mutation { deleteFoo(foo: {pk: 1, cc1: 1, cc2: 1}) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFoo")).isTrue();
    assertThat(exists(1, 1, 1)).isFalse();

    // Deleting a non-existing row always returns true:
    // when
    response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE, "mutation { deleteFoo(foo: {pk: 1, cc1: 1, cc2: 1}) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFoo")).isTrue();
  }

  @Test
  @DisplayName("Should delete rows by partial primary key")
  public void deleteByPartialPrimaryKey() {
    // Given
    insert(1, 1, 1);
    insert(1, 2, 2);
    insert(1, 3, 3);

    // when
    CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFoo2(pk: 1, cc1: 3) }");

    // then
    assertThat(exists(1, 1, 1)).isTrue();
    assertThat(exists(1, 2, 2)).isTrue();
    assertThat(exists(1, 3, 3)).isFalse();

    // when
    CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFoo2(pk: 1) }");

    // then
    assertThat(exists(1, 1, 1)).isFalse();
    assertThat(exists(1, 2, 2)).isFalse();
  }

  @Test
  @DisplayName("Should fail if not all partition keys are present")
  public void deleteWithMissingPartitionKey() {
    assertThat(CLIENT.getKeyspaceError(KEYSPACE, "mutation { deleteFoo2(cc1: 1, cc2: 1) }"))
        .contains(
            "Invalid arguments: every partition key field of type Foo must be present (expected: pk)");
  }

  @Test
  @DisplayName("Should fail if partial primary key is not a prefix")
  public void deleteByPartialPrimaryKeyNotPrefix() {
    assertThat(CLIENT.getKeyspaceError(KEYSPACE, "mutation { deleteFoo2(pk: 1, cc2: 1) }"))
        .contains(
            "Invalid arguments: clustering field cc1 is not restricted by EQ or IN, "
                + "so no other clustering field after it can be restricted (offending: cc2).");
  }

  @Test
  @DisplayName("Should delete single row by full primary key if exists")
  public void deleteByFullPrimaryKeyIfExists() {
    // Given
    insert(1, 1, 1);

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE, "mutation { deleteFooIfExists(foo: {pk: 1, cc1: 1, cc2: 1}) {applied} }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooIfExists.applied")).isTrue();
    assertThat(exists(1, 1, 1)).isFalse();

    // Deleting a non-existing row returns false:
    // when
    response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE, "mutation { deleteFooIfExists(foo: {pk: 1, cc1: 1, cc2: 1}) {applied} }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooIfExists.applied")).isFalse();
    assertThat(exists(1, 1, 1)).isFalse();
  }

  @Test
  @DisplayName("Should delete full partition with dedicated query")
  public void deleteFullPartitionDedicated() {
    // Given
    insert(1, 1, 1);
    insert(1, 2, 2);

    // When
    Object response =
        CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooPartition(pk: 1) }");

    // Then
    assertThat(exists(1, 1, 1)).isFalse();
    assertThat(exists(1, 2, 2)).isFalse();
  }

  @Test
  @DisplayName("Should delete full partition by not providing clustering arguments")
  public void deleteFullPartitionOmitClustering() {
    // Given
    insert(1, 1, 1);
    insert(1, 2, 2);

    // When
    // we call an operation that takes the whole PK, but only provide the partition key arguments
    Object response = CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFoo2(pk: 1) }");

    // Then
    assertThat(exists(1, 1, 1)).isFalse();
    assertThat(exists(1, 2, 2)).isFalse();
  }
}
