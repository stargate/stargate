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
package io.stargate.sgv2.graphql.integration.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.BetterBotzIntegrationTestBase;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class InsertIntegrationTest extends BetterBotzIntegrationTestBase {

  private static final String ID = UUID.randomUUID().toString();

  @AfterEach
  public void cleanup() {
    session.execute(String.format("DELETE FROM \"Products\" WHERE id = %s", ID));
  }

  @Test
  public void insertProduct() {
    Map<String, Object> response =
        insertProduct(
            ID, "Shiny Legs", "3199.99", "2011-02-02T20:05:00.000-08:00", "Normal legs but shiny.");
    assertThat(JsonPath.<Boolean>read(response, "$.insertProducts.applied")).isTrue();
    Map<String, Object> productInInsertResponse = JsonPath.read(response, "$.insertProducts.value");

    response = selectProduct(ID);
    Map<String, Object> productInSelectResponse = JsonPath.read(response, "$.Products.values[0]");

    for (Map<String, Object> product :
        Arrays.asList(productInInsertResponse, productInSelectResponse)) {
      assertIsProduct(
          product,
          ID,
          "Shiny Legs",
          "3199.99",
          "2011-02-02T20:05:00.000-08:00",
          "Normal legs but shiny.");
    }
  }

  @Test
  public void insertProductIfNotExistsApplied() {
    Map<String, Object> response =
        insertProductIfNotExists(
            ID, "Shiny Legs", "3199.99", "2011-02-02T20:05:00.000-08:00", "Normal legs but shiny.");
    assertThat(JsonPath.<Boolean>read(response, "$.insertProducts.applied")).isTrue();
    assertIsProduct(
        JsonPath.read(response, "$.insertProducts.value"),
        ID,
        "Shiny Legs",
        "3199.99",
        "2011-02-02T20:05:00.000-08:00",
        "Normal legs but shiny.");
  }

  @Test
  public void insertProductIfNotExistsNotApplied() {
    // Given a product that is already present
    insertProduct(
        ID, "Shiny Legs", "3199.99", "2011-02-02T20:05:00.000-08:00", "Normal legs but shiny.");

    // When trying to insert same id but different values
    Map<String, Object> response =
        insertProductIfNotExists(
            ID, "Shiny Legs", "3199.99", "2011-02-02T20:05:00.000-08:00", "New description.");

    // Then insert is not applied, and returns the original values
    assertThat(JsonPath.<Boolean>read(response, "$.insertProducts.applied")).isFalse();
    assertIsProduct(
        JsonPath.read(response, "$.insertProducts.value"),
        ID,
        "Shiny Legs",
        "3199.99",
        "2011-02-02T20:05:00.000-08:00",
        "Normal legs but shiny.");
  }

  private Map<String, Object> insertProduct(
      String id, String name, String price, String created, String description) {
    return insertProduct(id, name, price, created, description, false);
  }

  private Map<String, Object> insertProductIfNotExists(
      String id, String name, String price, String created, String description) {
    return insertProduct(id, name, price, created, description, true);
  }

  private Map<String, Object> insertProduct(
      String id,
      String name,
      String price,
      String created,
      String description,
      boolean ifNotExists) {
    return client.executeDmlQuery(
        keyspaceId.asInternal(),
        String.format(
            "mutation {\n"
                + "  insertProducts(\n"
                + "    value: {\n"
                + "      id: \"%s\"\n"
                + "      name: \"%s\"\n"
                + "      price: \"%s\"\n"
                + "      created: \"%s\"\n"
                + "      description: \"%s\"\n"
                + "    }\n,"
                + "    ifNotExists: %s"
                + "  ) {\n"
                + "    applied\n"
                + "    value { id, name, price, created, description }"
                + "  }\n"
                + "}",
            id, name, price, created, description, ifNotExists));
  }

  private Map<String, Object> selectProduct(String id) {
    return client.executeDmlQuery(
        keyspaceId.asInternal(),
        String.format(
            "{\n"
                + "  Products(\n"
                + "    value: { id: \"%s\" }"
                + "  ) {\n"
                + "    values { id, name, price, created, description }"
                + "  }\n"
                + "}",
            id));
  }
}
