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
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class SelectInvalidConditionsTest extends GraphqlFirstTestBase {

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
  @DisplayName("Should fail to deploy when list in EQ condition doesn't match list field")
  public void eqInvalidElementType() {
    // Given
    String invalidSchema =
        "type Foo { pk: [Int]! @cql_column(partitionKey: true) }\n"
            + "type Query {\n"
            + "  foosByPk(\n"
            // Should be [Int]:
            + "    pk: [String]\n"
            + "  ): [Foo]\n"
            + "}";

    // When
    List<Map<String, Object>> errors = CLIENT.getDeploySchemaErrors(KEYSPACE, null, invalidSchema);

    // Then
    assertThat(errors).hasSize(1);
    Map<String, Object> schemaError = JsonPath.read(errors.get(0), "$.extensions.mappingErrors[0]");
    assertThat(schemaError.get("message"))
        .asInstanceOf(InstanceOfAssertFactories.STRING)
        .isEqualTo("Operation foosByPk: expected argument pk to have type [Int] to match Foo.pk");
  }

  @Test
  @DisplayName("Should fail to deploy when element type of IN condition doesn't match field")
  public void inInvalidElementType() {
    // Given
    String invalidSchema =
        "type Foo { pk: Int! @cql_column(partitionKey: true) }\n"
            + "type Query {\n"
            + "  foosByPks(\n"
            // Should be [Int]:
            + "    pks: [String] @cql_where(field: \"pk\", predicate: IN)\n"
            + "  ): [Foo]\n"
            + "}";

    // When
    List<Map<String, Object>> errors = CLIENT.getDeploySchemaErrors(KEYSPACE, null, invalidSchema);

    // Then
    assertThat(errors).hasSize(1);
    Map<String, Object> schemaError = JsonPath.read(errors.get(0), "$.extensions.mappingErrors[0]");
    assertThat(schemaError.get("message"))
        .asInstanceOf(InstanceOfAssertFactories.STRING)
        .isEqualTo("Operation foosByPks: expected argument pks to have type [Int] to match Foo.pk");
  }

  @Test
  @DisplayName(
      "Should fail to deploy when type of CONTAINS condition doesn't match element type of field")
  public void containsInvalidType() {
    // Given
    String invalidSchema =
        "type Foo {\n"
            + "  pk: Int! @cql_column(partitionKey: true)\n"
            + "  l: [[Int]] @cql_index\n"
            + "}\n"
            + "type Query {\n"
            + "  foosByL(\n"
            // Should be [Int] because it represents an element of [[Int]]:
            + "    l: [String] @cql_where(predicate: CONTAINS)\n"
            + "  ): [Foo]\n"
            + "}";

    // When
    List<Map<String, Object>> errors = CLIENT.getDeploySchemaErrors(KEYSPACE, null, invalidSchema);

    // Then
    assertThat(errors).hasSize(1);
    Map<String, Object> schemaError = JsonPath.read(errors.get(0), "$.extensions.mappingErrors[0]");
    assertThat(schemaError.get("message"))
        .asInstanceOf(InstanceOfAssertFactories.STRING)
        .isEqualTo(
            "Operation foosByL: expected argument l to have type [Int] to match element type of Foo.l");
  }
}
