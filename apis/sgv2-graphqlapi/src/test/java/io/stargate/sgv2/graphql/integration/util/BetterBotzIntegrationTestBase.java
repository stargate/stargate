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
package io.stargate.sgv2.graphql.integration.util;

import com.jayway.jsonpath.JsonPath;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;

import static org.assertj.core.api.Assertions.assertThat;

/** Base class for GraphQL CQL-first tests that share the data model below. */
public abstract class BetterBotzIntegrationTestBase extends CqlFirstIntegrationTest {

  private static final DateTimeFormatter INSTANT_PARSER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

  @BeforeAll
  public final void createSchema() {
    executeCql(
        "CREATE TABLE IF NOT EXISTS \"Products\" (\n"
            + "    id uuid,\n"
            + "    name text,\n"
            + "    price decimal,\n"
            + "    created timestamp,\n"
            + "    \"prodName\" text,\n"
            + "    \"customerName\" text,\n"
            + "    description text,\n"
            + "    PRIMARY KEY ((id), name, price, created)\n"
            + ")");
    executeCql(
        "CREATE TABLE IF NOT EXISTS \"Orders\" (\n"
            + "    \"prodName\" text,\n"
            + "    \"customerName\" text,\n"
            + "    id uuid,\n"
            + "    \"prodId\" uuid,\n"
            + "    address text,\n"
            + "    description text,\n"
            + "    price decimal,\n"
            + "    \"sellPrice\" decimal,\n"
            + "\"value_int\" int,\n"
            + "    \"value_double\" double,\n"
            + "    \"value_bigint\" bigint,\n"
            + "    \"value_varint\" varint,\n"
            + "    \"value_float\" float,\n"
            + "    \"value_smallint\" smallint,\n"
            + "    \"value_tinyint\" tinyint,"
            + "    PRIMARY KEY ((\"prodName\"), \"customerName\")\n"
            + ")");
    executeCql(
        "INSERT INTO \"Orders\" "
            + "(id, \"prodId\", \"prodName\", description, price, "
            + "\"sellPrice\", \"customerName\", address) VALUES ("
            + "792d0a56-bb46-4bc2-bc41-5f4a94a83da9,"
            + "31047029-2175-43ce-9fdd-b3d568b19bb2,"
            + "'Medium Lift Arms',"
            + "'Ordering some more arms for my construction bot.',"
            + "3199.99,"
            + "3119.99,"
            + "'Janice Evernathy',"
            + "'2101 Everplace Ave 3116')");
    executeCql(
        "INSERT INTO \"Orders\" "
            + "(id, \"prodId\", \"prodName\", description, price, "
            + "\"sellPrice\", \"customerName\", address) VALUES ("
            + "dd73afe2-9841-4ce1-b841-575b8be405c1,"
            + "31047029-2175-43ce-9fdd-b3d568b19bb5,"
            + "'Basic Task CPU',"
            + "'Ordering replacement CPUs.',"
            + "899.99,"
            + "900.82,"
            + "'John Doe',"
            + "'123 Main St 67890')");
  }

  /** Asserts that a JSON snippet represents an order with the given data. */
  protected void assertIsOrder(
      Map<String, Object> json,
      String expectedId,
      String expectedProdId,
      String expectedProdName,
      String expectedCustomerName,
      String expectedAddress,
      String expectedDescription,
      String expectedPrice,
      String expectedSellPrice) {

    assertThat(JsonPath.<String>read(json, "id")).isEqualTo(expectedId);
    assertThat(JsonPath.<String>read(json, "prodId")).isEqualTo(expectedProdId);
    assertThat(JsonPath.<String>read(json, "prodName")).isEqualTo(expectedProdName);
    assertThat(JsonPath.<String>read(json, "customerName")).isEqualTo(expectedCustomerName);
    assertThat(JsonPath.<String>read(json, "address")).isEqualTo(expectedAddress);
    assertThat(JsonPath.<String>read(json, "description")).isEqualTo(expectedDescription);
    assertThat(JsonPath.<String>read(json, "price")).isEqualTo(expectedPrice);
    assertThat(JsonPath.<String>read(json, "sellPrice")).isEqualTo(expectedSellPrice);
  }

  /** Asserts that a JSON snippet represents a product with the given data. */
  protected void assertIsProduct(
      Map<String, Object> json,
      String expectedId,
      String expectedName,
      String expectedPrice,
      String expectedCreated,
      String expectedDescription) {

    assertThat(JsonPath.<String>read(json, "id")).isEqualTo(expectedId);
    assertThat(JsonPath.<String>read(json, "name")).isEqualTo(expectedName);
    assertThat(JsonPath.<String>read(json, "price")).isEqualTo(expectedPrice);
    assertThat(parseInstant(JsonPath.read(json, "created")))
        .isEqualTo(parseInstant(expectedCreated));
    assertThat(JsonPath.<String>read(json, "description")).isEqualTo(expectedDescription);
  }

  private Instant parseInstant(String spec) {
    return Instant.from(INSTANT_PARSER.parse(spec));
  }
}
