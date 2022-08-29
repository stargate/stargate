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
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AsyncDirectiveIntegrationTest extends BetterBotzIntegrationTestBase {

  private static final String FIRST_PRODUCT_ID = UUID.randomUUID().toString();
  private static final String SECOND_PRODUCT_ID = UUID.randomUUID().toString();

  @AfterEach
  public void cleanup() {
    session.execute(String.format("DELETE FROM \"Products\" WHERE id = %s", FIRST_PRODUCT_ID));
    session.execute(String.format("DELETE FROM \"Products\" WHERE id = %s", SECOND_PRODUCT_ID));
  }

  @Test
  @DisplayName("Should insert using async directive")
  public void shouldInsertWhenUsingAsyncDirective() {
    Map<String, Object> response = insertProductWithAsyncDirective(FIRST_PRODUCT_ID);
    assertThat(JsonPath.<Boolean>read(response, "$.insertProducts.accepted")).isTrue();
  }

  @Test
  @DisplayName("Should insert using both async and atomic directives")
  public void shouldInsertWhenUsingBothAsyncAndAtomicDirectives() {
    Map<String, Object> response = insertProductWithAsyncAndAtomicDirectives(FIRST_PRODUCT_ID);
    assertThat(JsonPath.<Boolean>read(response, "$.insertProducts.accepted")).isTrue();
  }

  @Test
  @DisplayName("Should bulk insert using async directive")
  public void shouldBulkInsertWhenUsingAsyncDirective() {
    Map<String, Object> response =
        bulkInsertProductsWithAsyncDirective(FIRST_PRODUCT_ID, SECOND_PRODUCT_ID);
    assertThat(JsonPath.<Boolean>read(response, "$.bulkInsertProducts[0].accepted")).isTrue();
    assertThat(JsonPath.<Boolean>read(response, "$.bulkInsertProducts[1].accepted")).isTrue();
  }

  @Test
  @DisplayName("Should bulk insert using both async and atomic directives")
  public void shouldBulkInsertWhenUsingBothAsyncAndAtomicDirective() {
    Map<String, Object> response =
        bulkInsertProductsWithAsyncAndAtomicDirectives(FIRST_PRODUCT_ID, SECOND_PRODUCT_ID);
    assertThat(JsonPath.<Boolean>read(response, "$.bulkInsertProducts[0].accepted")).isTrue();
    assertThat(JsonPath.<Boolean>read(response, "$.bulkInsertProducts[1].accepted")).isTrue();
  }

  private Map<String, Object> bulkInsertProductsWithAsyncAndAtomicDirectives(
      String firstProductId, String secondProductId) {
    return bulkInsertProductWithDirectives(firstProductId, secondProductId, "@async @atomic");
  }

  private Map<String, Object> bulkInsertProductsWithAsyncDirective(
      String firstProductId, String secondProductId) {
    return bulkInsertProductWithDirectives(firstProductId, secondProductId, "@async");
  }

  private Map<String, Object> insertProductWithAsyncDirective(String productId) {
    return insertProductWithDirectives(productId, "@async");
  }

  private Map<String, Object> insertProductWithAsyncAndAtomicDirectives(String productId) {
    return insertProductWithDirectives(productId, "@async @atomic");
  }

  private Map<String, Object> insertProductWithDirectives(String id, String directives) {
    return client.executeDmlQuery(
        keyspaceId.asInternal(),
        String.format(
            "mutation %s {\n"
                + "  insertProducts(\n"
                + "    value: {\n"
                + "      id: \"%s\"\n"
                + "      name: \"%s\"\n"
                + "      price: \"%s\"\n"
                + "      created: \"%s\"\n"
                + "      description: \"%s\"\n"
                + "    }\n,"
                + "    ifNotExists: true"
                + "  ) {\n"
                + "    accepted\n"
                + "  }\n"
                + "}",
            directives,
            id,
            "Shiny Legs",
            "3199.99",
            "2011-02-02T20:05:00.000-08:00",
            "Normal legs but shiny."));
  }

  private Map<String, Object> bulkInsertProductWithDirectives(
      String firstProductId, String secondProductId, String directives) {
    return client.executeDmlQuery(
        keyspaceId.asInternal(),
        String.format(
            "mutation %s {\n"
                + "  bulkInsertProducts(\n"
                + "    values: [{\n"
                + "      id: \"%s\"\n"
                + "      name: \"%s\"\n"
                + "      price: \"%s\"\n"
                + "      created: \"%s\"\n"
                + "      description: \"%s\"\n"
                + "    }, \n"
                + "    {\n"
                + "      id: \"%s\"\n"
                + "      name: \"%s\"\n"
                + "      price: \"%s\"\n"
                + "      created: \"%s\"\n"
                + "      description: \"%s\"\n"
                + "    }\n,"
                + "]\n,"
                + "    ifNotExists: true"
                + "  ) {\n"
                + "    accepted\n"
                + "  }\n"
                + "}",
            directives,
            firstProductId,
            "Shiny Legs",
            "3199.99",
            "2011-02-02T20:05:00.000-08:00",
            "Normal legs but shiny.",
            secondProductId,
            "Other product",
            "3000.99",
            "2012-02-02T20:05:00.000-08:00",
            "Other legs."));
  }
}
