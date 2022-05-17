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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.ApiServiceConnectionInfo;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
public class TtlTest extends GraphqlFirstTestBase {

  private static GraphqlFirstClient CLIENT;
  private static String KEYSPACE;
  private static UUID SCHEMA_VERSION;

  @BeforeAll
  public static void setup(
      StargateConnectionInfo stargateBackend,
      ApiServiceConnectionInfo stargateGraphqlApi,
      @TestKeyspace CqlIdentifier keyspaceId) {
    CLIENT =
        new GraphqlFirstClient(
            stargateGraphqlApi.host(),
            stargateGraphqlApi.port(),
            RestUtils.getAuthToken(stargateBackend.seedAddress()));
    KEYSPACE = keyspaceId.asInternal();
    SCHEMA_VERSION =
        CLIENT.deploySchema(
            KEYSPACE,
            "type Foo @cql_input{\n"
                + "  k: Int @cql_column(partitionKey:true)\n"
                + "  v: Int\n"
                + "}\n"
                + "type Mutation {\n"
                + "  insertFoo(foo: FooInput): Foo @cql_insert(ttl: \"3600\")\n"
                + "  updateFoo(k: Int, v: Int): Boolean @cql_update(targetEntity: \"Foo\", ttl: \"PT2H\")\n"
                + "}\n"
                + "type Query {\n"
                + "  foo(k: Int): Foo\n"
                + "}");
  }

  @Test
  @DisplayName("Should insert with TTL")
  public void insertTest(CqlSession session) {
    // Given
    CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { insertFoo(foo:{k:1, v:1}) {v} }");

    // When
    Integer ttl = session.execute("SELECT ttl(v) FROM \"Foo\" WHERE k = 1").one().getInt(0);

    // Then
    assertThat(ttl).isCloseTo(3600, offset(60));
  }

  @Test
  @DisplayName("Should update with TTL")
  public void updateTest(CqlSession session) {
    // Given
    CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { updateFoo(k:2, v:1) }");

    // When
    Integer ttl = session.execute("SELECT ttl(v) FROM \"Foo\" WHERE k = 2").one().getInt(0);

    // Then
    assertThat(ttl).isCloseTo(7200, offset(60));
  }

  @ParameterizedTest
  @MethodSource("malformedTtls")
  @DisplayName("Should fail to deploy if TTL is malformed")
  public void malformedTtlTest(String ttl, String expectedError) {
    String schema =
        String.format(
            "type Foo @cql_input{\n"
                + "  k: Int @cql_column(partitionKey:true)\n"
                + "  v: Int\n"
                + "}\n"
                + "type Mutation {\n"
                + "  insertFoo(foo: FooInput): Foo @cql_insert(ttl: \"%s\")\n"
                + "}\n"
                + "type Query {\n"
                + "  foo(k: Int): Foo\n"
                + "}",
            ttl);
    List<Map<String, Object>> errors =
        CLIENT.getDeploySchemaErrors(KEYSPACE, SCHEMA_VERSION.toString(), schema);
    assertThat(errors).hasSize(1);
    Map<String, Object> schemaError = JsonPath.read(errors.get(0), "$.extensions.mappingErrors[0]");
    assertThat(schemaError.get("message"))
        .asInstanceOf(InstanceOfAssertFactories.STRING)
        .contains(expectedError);
  }

  @SuppressWarnings("unused")
  public static Arguments[] malformedTtls() {
    return new Arguments[] {
      arguments("abc", "can't parse TTL 'abc' (expected an integer or ISO-8601 duration string)"),
      arguments("-1", "TTL must between 0 and 2^31 - 1 seconds"),
      arguments("-PT20S", "TTL must between 0 and 2^31 - 1 seconds"),
      arguments("2147483648", "TTL must between 0 and 2^31 - 1 seconds"),
      arguments("P25000D", "TTL must between 0 and 2^31 - 1 seconds"),
    };
  }
}
