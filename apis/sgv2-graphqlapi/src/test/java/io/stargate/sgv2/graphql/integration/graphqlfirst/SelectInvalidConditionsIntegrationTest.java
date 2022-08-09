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
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.util.List;
import java.util.Map;
import javax.enterprise.context.control.ActivateRequestContext;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SelectInvalidConditionsIntegrationTest extends GraphqlFirstIntegrationTest {

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
    List<Map<String, Object>> errors =
        client.getDeploySchemaErrors(keyspaceName, null, invalidSchema);

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
    List<Map<String, Object>> errors =
        client.getDeploySchemaErrors(keyspaceName, null, invalidSchema);

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
    List<Map<String, Object>> errors =
        client.getDeploySchemaErrors(keyspaceName, null, invalidSchema);

    // Then
    assertThat(errors).hasSize(1);
    Map<String, Object> schemaError = JsonPath.read(errors.get(0), "$.extensions.mappingErrors[0]");
    assertThat(schemaError.get("message"))
        .asInstanceOf(InstanceOfAssertFactories.STRING)
        .isEqualTo(
            "Operation foosByL: expected argument l to have type [Int] to match element type of Foo.l");
  }
}
