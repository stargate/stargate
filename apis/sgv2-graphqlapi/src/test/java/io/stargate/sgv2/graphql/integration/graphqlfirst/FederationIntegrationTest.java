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

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FederationIntegrationTest extends GraphqlFirstIntegrationTest {

  private static final String SCHEMA =
      "type Entity1 @key { k: ID! }\n"
          + "type Entity2 @key(fields: \"k\") { k: Int! @cql_column(partitionKey: true) }\n"
          + "type Entity3 @key {\n"
          + "  k1: Int! @cql_column(partitionKey: true)\n"
          + "  k2: Int! @cql_column(partitionKey: true)\n"
          + "  cc1: Int! @cql_column(clusteringOrder: ASC)\n"
          + "  cc2: Int! @cql_column(clusteringOrder: ASC)\n"
          + "}\n"
          + "type Key @cql_entity(target: UDT) { k: Int }\n"
          + "type Entity4 @key { k: Key! @cql_column(partitionKey: true) }";

  private static final UUID UUID_KEY = UUID.randomUUID();

  @BeforeAll
  public void deploySchema() {
    client.deploySchema(keyspaceId.asInternal(), SCHEMA);

    session.execute("INSERT INTO \"Entity1\" (k) VALUES (%s)".formatted(UUID_KEY));
    session.execute("INSERT INTO \"Entity2\" (k) VALUES (1)");
    session.execute("INSERT INTO \"Entity3\" (k1,k2,cc1,cc2) VALUES (1,2,3,4)");
    session.execute("INSERT INTO \"Entity4\" (k) VALUES ({k: 1})");
  }

  @Test
  @DisplayName("Should fetch entity with ID key")
  public void idKeyTest() {
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "query {\n"
                + "_entities(representations: [ "
                + String.format("{ __typename: \"Entity1\", k: \"%s\" }, ", UUID_KEY)
                + " ]) { "
                + "... on Entity1 { k } "
                + "} }");

    assertThat(JsonPath.<String>read(response, "$._entities[0].k")).isEqualTo(UUID_KEY.toString());
  }

  @Test
  @DisplayName("Should fetch entity with Int key")
  public void intKeyTest() {
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "query {\n"
                + "_entities(representations: [ "
                + "{ __typename: \"Entity2\", k: 1 }, "
                + " ]) { "
                + "... on Entity2 { k } "
                + "} }");

    assertThat(JsonPath.<Integer>read(response, "$._entities[0].k")).isEqualTo(1);
  }

  @Test
  @DisplayName("Should fetch entity with composite key")
  public void compositeKeyTest() {
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "query {\n"
                + "_entities(representations: [ "
                + "{ __typename: \"Entity3\", k1: 1, k2: 2, cc1: 3, cc2: 4 }, "
                + " ]) { "
                + "... on Entity3 { k1, k2, cc1, cc2 } "
                + "} }");

    assertThat(JsonPath.<Integer>read(response, "$._entities[0].k1")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$._entities[0].k2")).isEqualTo(2);
    assertThat(JsonPath.<Integer>read(response, "$._entities[0].cc1")).isEqualTo(3);
    assertThat(JsonPath.<Integer>read(response, "$._entities[0].cc2")).isEqualTo(4);
  }

  @Test
  @DisplayName("Should fetch entity with UDT key")
  public void udtKeyTest() {
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "query {\n"
                + "_entities(representations: [ "
                + "{ __typename: \"Entity4\", k: { k: 1 } }, "
                + " ]) { "
                + "... on Entity4 { k { k } } "
                + "} }");

    assertThat(JsonPath.<Integer>read(response, "$._entities[0].k.k")).isEqualTo(1);
  }

  @Test
  @DisplayName("Should include trace if requested via header")
  public void federatedTracing() {
    Object response =
        client.getKeyspaceFullResponse(
            Collections.singletonMap("apollo-federation-include-trace", "ftv1"),
            keyspaceId.asInternal(),
            "query {\n"
                + "_entities(representations: [ "
                + "{ __typename: \"Entity2\", k: 1 }, "
                + " ]) { "
                + "... on Entity2 { k } "
                + "} }");

    String trace = JsonPath.read(response, "$.extensions.ftv1");
    // The value is an opaque string (Base64 encoding of the protobuf representation of the trace).
    // Don't attempt to decode it here: we are not testing the federation-jvm code that builds the
    // trace, just the fact that it's wired correctly into Stargate.
    assertThat(trace).isNotEmpty();
  }
}
