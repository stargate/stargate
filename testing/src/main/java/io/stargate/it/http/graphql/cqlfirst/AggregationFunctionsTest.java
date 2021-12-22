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
package io.stargate.it.http.graphql.cqlfirst;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class AggregationFunctionsTest extends BetterbotzTestBase {
  private static CqlFirstClient CLIENT;
  private static CqlIdentifier KEYSPACE_ID;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceId) {
    String host = cluster.seedAddress();
    CLIENT = new CqlFirstClient(host, RestUtils.getAuthToken(host));
    KEYSPACE_ID = keyspaceId;
  }

  @AfterEach
  public void cleanup(CqlSession session) {
    session.execute("TRUNCATE TABLE \"Orders\"");
  }

  @Test
  @DisplayName("Should calculate number of orders using count aggregation")
  public void countUsingBigIntFunctionWithAlias() {
    insertOrder("p1", "c1", "3000", "d1");
    insertOrder("p1", "c2", "2500", "d1");

    Map<String, Object> result = getOrderWithCount("p1");
    assertThat(JsonPath.<String>read(result, "$.Orders.values[0].count")).isEqualTo("2");
  }

  @Test
  @DisplayName("Should calculate average of orders' price using avg aggregation")
  public void avgUsingDecimalFunctionWithoutAlias() {
    insertOrder("p1", "c1", "4", "d1");
    insertOrder("p1", "c2", "10", "d1");

    Map<String, Object> result = getOrderWithAvg("p1");
    // avg price is (4 + 10) / 2 = 7
    assertThat(JsonPath.<String>read(result, "$.Orders.values[0]._decimal_function"))
        .isEqualTo("7");
  }

  @Test
  @DisplayName("Should get the highest order using max aggregation")
  public void maxUsingDecimalFunctionWithAlias() {
    insertOrder("p1", "c1", "2500", "d1");
    insertOrder("p1", "c2", "3000", "d1");

    Map<String, Object> result = getOrderWithMax("p1");
    assertThat(JsonPath.<String>read(result, "$.Orders.values[0].max")).isEqualTo("3000");
  }

  @Test
  @DisplayName("Should get the lowest order using min aggregation")
  public void minUsingDecimalFunctionWithAlias() {
    insertOrder("p1", "c1", "2500", "d1");
    insertOrder("p1", "c2", "3000", "d1");

    Map<String, Object> result = getOrderWithMin("p1");
    assertThat(JsonPath.<String>read(result, "$.Orders.values[0].min")).isEqualTo("2500");
  }

  @Test
  @DisplayName("Should sum all orders' price using sum aggregation")
  public void sumUsingDecimalFunctionWithAlias() {
    insertOrder("p1", "c1", "2500", "d1");
    insertOrder("p1", "c2", "3000", "d1");

    Map<String, Object> result = getOrderWithSum("p1");
    assertThat(JsonPath.<String>read(result, "$.Orders.values[0].sum")).isEqualTo("5500");
  }

  @Test
  @DisplayName("Should test all available function types")
  public void testAllGraphqlFunctionTypes() {
    Number value = 100;
    insertOrder("p1", "c1", "d1", value);

    Map<String, Object> result = getOrderWithAllFunctions("p1");
    assertThat(JsonPath.<Integer>read(result, "$.Orders.values[0]._int_function"))
        .isEqualTo(value.intValue());
    assertThat(JsonPath.<Double>read(result, "$.Orders.values[0]._double_function"))
        .isEqualTo(value.doubleValue());
    assertThat(JsonPath.<String>read(result, "$.Orders.values[0]._bigint_function"))
        .isEqualTo(value.toString());
    assertThat(JsonPath.<String>read(result, "$.Orders.values[0]._decimal_function"))
        .isEqualTo(value.toString());
    assertThat(JsonPath.<String>read(result, "$.Orders.values[0]._varint_function"))
        .isEqualTo(value.toString());
    assertThat(JsonPath.<Double>read(result, "$.Orders.values[0]._float_function"))
        .isEqualTo(value.doubleValue());
    assertThat(JsonPath.<Integer>read(result, "$.Orders.values[0]._smallint_function"))
        .isEqualTo(value.shortValue());
    assertThat(JsonPath.<Integer>read(result, "$.Orders.values[0]._tinyint_function"))
        .isEqualTo(value.byteValue());
  }

  @Test
  @DisplayName("Should error when trying to use an unknown aggregation function")
  public void shouldErrorWhenTryingToUseAnUnknownAggregationFunction() {
    insertOrder("p1", "c1", "2500", "d1");

    String result = getOrderWithUnknownFunction("p1");

    assertThat(result).contains("Column 'unknown_function' is not defined in the Row's metadata.");
  }

  @Test
  @DisplayName("Should error when trying to use an unknown graphql function return type")
  public void shouldErrorWhenTryingToUseAnUnknownGraphqlFunctionReturnType() {
    insertOrder("p1", "c1", "2500", "d1");

    String result = getOrderWithUnknownFunctionReturnType("p1");

    assertThat(result)
        .contains(
            "Validation error of type FieldUndefined: Field '_unknown_function' in type 'Orders' is undefined");
  }

  @Test
  @DisplayName("Should calculate the sum of orders sellPrices using sum on a case sensitive column")
  public void sumOnCaseSensitiveColumn() {
    insertOrder("p1", "c1", "3000", "d1");
    insertOrder("p1", "c2", "2500", "d1");

    Map<String, Object> result = getOrderWithSumCaseSensitive("p1");
    assertThat(JsonPath.<String>read(result, "$.Orders.values[0].sum")).isEqualTo("5500");
  }

  private Map<String, Object> insertOrder(
      String prodName, String customerName, String description, Number value) {
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            KEYSPACE_ID,
            String.format(
                "mutation {\n"
                    + "  insertOrders(\n"
                    + "    value: {\n"
                    + "      prodName: \"%s\"\n"
                    + "      customerName: \"%s\"\n"
                    + "      price: \"%s\"\n"
                    + "      value_int: %s\n"
                    + "      value_double: %s\n"
                    + "      value_bigint: \"%s\"\n"
                    + "      value_varint: \"%s\"\n"
                    + "      value_float: %s\n"
                    + "      value_smallint: %s\n"
                    + "      value_tinyint: %s\n"
                    + "      description: \"%s\"\n"
                    + "    }\n,"
                    + "    ifNotExists: true"
                    + "  ) {\n"
                    + "    applied\n"
                    + "    value { id, prodId, prodName, customerName, address, description, price, sellPrice }"
                    + "  }\n"
                    + "}",
                prodName,
                customerName,
                value.toString(),
                value.intValue(),
                value.doubleValue(),
                value,
                value,
                value.doubleValue(),
                value.shortValue(),
                value.byteValue(),
                description));
    assertThat(JsonPath.<Boolean>read(response, "$.insertOrders.applied")).isTrue();
    return response;
  }

  private Map<String, Object> insertOrder(
      String prodName, String customerName, String price, String description) {
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            KEYSPACE_ID,
            String.format(
                "mutation {\n"
                    + "  insertOrders(\n"
                    + "    value: {\n"
                    + "      prodName: \"%s\"\n"
                    + "      customerName: \"%s\"\n"
                    + "      price: \"%s\"\n"
                    + "      sellPrice: \"%s\"\n"
                    + "      description: \"%s\"\n"
                    + "    }\n,"
                    + "    ifNotExists: true"
                    + "  ) {\n"
                    + "    applied\n"
                    + "    value { id, prodId, prodName, customerName, address, description, price, sellPrice }"
                    + "  }\n"
                    + "}",
                prodName, customerName, price, price, description));
    assertThat(JsonPath.<Boolean>read(response, "$.insertOrders.applied")).isTrue();
    return response;
  }

  private Map<String, Object> getOrderWithCount(String prodName) {
    return getOrderWithFunction(
        prodName, "count: _bigint_function(name: \"count\", args: [\"description\"])");
  }

  private Map<String, Object> getOrderWithMax(String prodName) {
    return getOrderWithFunction(
        prodName, "max: _decimal_function(name: \"max\", args: [\"price\"])");
  }

  private String getOrderWithUnknownFunction(String prodName) {
    return getOrderWithFunctionError(
        prodName,
        "unknown_function: _decimal_function(name: \"some_unknown_function\", args: [\"price\"])");
  }

  private String getOrderWithUnknownFunctionReturnType(String prodName) {
    return getOrderWithFunctionError(
        prodName,
        "unknown_function_return_type: _unknown_function(name: \"count\", args: [\"price\"])");
  }

  private Map<String, Object> getOrderWithMin(String prodName) {
    return getOrderWithFunction(
        prodName, "min: _decimal_function(name: \"min\", args: [\"price\"])");
  }

  private Map<String, Object> getOrderWithAvg(String prodName) {
    return getOrderWithFunction(prodName, "_decimal_function(name: \"avg\", args: [\"price\"])");
  }

  private Map<String, Object> getOrderWithSum(String prodName) {
    return getOrderWithFunction(
        prodName, "sum: _decimal_function(name: \"sum\", args: [\"price\"])");
  }

  private Map<String, Object> getOrderWithSumCaseSensitive(String prodName) {
    return getOrderWithFunction(
        prodName, "sum: _decimal_function(name: \"sum\", args: [\"sellPrice\"])");
  }

  private Map<String, Object> getOrderWithAllFunctions(String prodName) {
    return getOrderWithFunction(
        prodName,
        "_int_function(name: \"max\", args: [\"value_int\"])\n"
            + "            _double_function(name: \"max\", args: [\"value_double\"])\n"
            + "            _bigint_function(name: \"max\", args: [\"value_bigint\"])\n"
            + "            _decimal_function(name: \"max\", args: [\"price\"])\n"
            + "            _varint_function(name: \"max\", args: [\"value_varint\"])\n"
            + "            _float_function(name: \"max\", args: [\"value_float\"])\n"
            + "            _smallint_function(name: \"max\", args: [\"value_smallint\"])\n"
            + "            _tinyint_function(name: \"max\", args: [\"value_tinyint\"])");
  }

  private Map<String, Object> getOrderWithFunction(String prodName, String function) {
    return CLIENT.executeDmlQuery(KEYSPACE_ID, createQueryWithFunction(prodName, function));
  }

  private String getOrderWithFunctionError(String prodName, String function) {
    return CLIENT.getDmlQueryError(KEYSPACE_ID, createQueryWithFunction(prodName, function));
  }

  private String createQueryWithFunction(String prodName, String function) {
    return String.format(
        "{\n"
            + "  Orders(\n"
            + "    filter: {\n"
            + "      prodName: { eq: \"%s\" }\n"
            + "    }\n"
            + "  ) {\n"
            + "    values {\n"
            + "       %s\n"
            + "    }\n"
            + "  }\n"
            + "}",
        prodName, function);
  }
}
