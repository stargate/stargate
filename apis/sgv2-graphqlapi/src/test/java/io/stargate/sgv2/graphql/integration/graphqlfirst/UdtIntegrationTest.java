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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.util.Arrays;
import java.util.Optional;
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
public class UdtIntegrationTest extends GraphqlFirstIntegrationTest {

  @BeforeAll
  public void deploySchema() {
    client.deploySchema(
        keyspaceId.asInternal(),
        "type Key @cql_entity(target: UDT) @cql_input {\n"
            + "  k: Int\n"
            + "}\n"
            + "type Value @cql_entity(target: UDT) @cql_input {\n"
            + "  v: Int\n"
            + "}\n"
            + "type Foo @cql_input {\n"
            // UDT as partition key
            + "  k: Key! @cql_column(partitionKey: true, typeHint: \"frozen<\\\"Key\\\">\")\n"
            // UDT as a regular column with index => must be frozen
            + "  v: Value @cql_column(typeHint: \"frozen<\\\"Value\\\">\") @cql_index\n"
            // List of UDT (the element is implicitly frozen) with index on elements
            + "  vs1: [Value] @cql_index\n"
            // List of UDT with FULL index => the list must be frozen
            + "  vs2: [Value]  @cql_column(typeHint: \"frozen<list<\\\"Value\\\">>\")\n"
            + "                @cql_index(target: FULL)\n"
            + "}\n"
            + "type Query {\n"
            + "  foo(k: KeyInput!): Foo\n"
            + "  foosByV(v: ValueInput): [Foo]\n"
            + "  foosByVs1(\n"
            + "    v: ValueInput @cql_where(field: \"vs1\" predicate: CONTAINS)\n"
            + "  ): [Foo]\n"
            + "  foosByVs2(\n"
            + "    vs2: [ValueInput]\n"
            + "  ): [Foo]\n"
            + "}\n");

    Optional<KeyspaceMetadata> keyspace = session.refreshSchema().getKeyspace(keyspaceId);
    UserDefinedType keyType =
        keyspace
            .flatMap(ks -> ks.getUserDefinedType(CqlIdentifier.fromInternal("Key")))
            .orElseThrow(AssertionError::new);
    UserDefinedType valueType =
        keyspace
            .flatMap(ks -> ks.getUserDefinedType(CqlIdentifier.fromInternal("Value")))
            .orElseThrow(AssertionError::new);
    // Just insert one row. We just want to check that the queries run and things get serialized and
    // deserialized correctly, we're not testing the backend.
    session.execute(
        "INSERT INTO \"Foo\"(k, v, vs1, vs2) VALUES(?, ?, ?, ?)",
        keyType.newValue(1),
        valueType.newValue(2),
        Arrays.asList(valueType.newValue(3)),
        Arrays.asList(valueType.newValue(4)));
  }

  @Test
  @DisplayName("Should query by UDT primary key")
  public void queryByPrimaryKey() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "query { result: foo(k: {k: 1}) { k { k } } }");

    // then
    assertThat(JsonPath.<Integer>read(response, "$.result.k.k")).isEqualTo(1);
  }

  @Test
  @DisplayName("Should query by indexed UDT column")
  public void queryByIndex() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "query { result: foosByV(v: {v: 2}) { k { k } } }");

    // then
    assertThat(JsonPath.<Integer>read(response, "$.result[0].k.k")).isEqualTo(1);
  }

  @Test
  @DisplayName("Should query by CONTAINS in indexed UDT list")
  public void queryListContains() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "query { result: foosByVs1(v: {v: 3}) { k { k } } }");

    // then
    assertThat(JsonPath.<Integer>read(response, "$.result[0].k.k")).isEqualTo(1);
  }

  @Test
  @DisplayName("Should query by FULL indexed UDT list")
  public void queryListFull() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "query { result: foosByVs2(vs2: [{v: 4}]) { k { k } } }");

    // then
    assertThat(JsonPath.<Integer>read(response, "$.result[0].k.k")).isEqualTo(1);
  }
}
