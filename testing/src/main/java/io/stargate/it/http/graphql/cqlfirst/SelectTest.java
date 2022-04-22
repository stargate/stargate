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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.ApiServiceConnectionInfo;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SelectTest extends BetterbotzTestBase {

  private static CqlFirstClient CLIENT;

  @BeforeAll
  public static void setup(
      StargateConnectionInfo stargateBackend, ApiServiceConnectionInfo stargateGraphqlApi) {
    CLIENT =
        new CqlFirstClient(
            stargateGraphqlApi.host(),
            stargateGraphqlApi.port(),
            RestUtils.getAuthToken(stargateBackend.seedAddress()));
  }

  @Test
  public void getOrdersByValue(@TestKeyspace CqlIdentifier keyspaceId) {
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "{\n"
                + "  Orders(value: {prodName: \"Medium Lift Arms\"}) {\n"
                + "    values {\n"
                + "      id, prodId, prodName, customerName, address,\n"
                + "      description, price, sellPrice\n"
                + "    }\n"
                + "  }\n"
                + "}");
    assertIsOrder(
        JsonPath.read(response, "$.Orders.values[0]"),
        "792d0a56-bb46-4bc2-bc41-5f4a94a83da9",
        "31047029-2175-43ce-9fdd-b3d568b19bb2",
        "Medium Lift Arms",
        "Janice Evernathy",
        "2101 Everplace Ave 3116",
        "Ordering some more arms for my construction bot.",
        "3199.99",
        "3119.99");
  }

  @Test
  public void getOrdersWithFilter(@TestKeyspace CqlIdentifier keyspaceId) {
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "{\n"
                + "  Orders(\n"
                + "    filter: {\n"
                + "      prodName: { eq: \"Basic Task CPU\" }\n"
                + "      customerName: { eq: \"John Doe\" }\n"
                + "    }\n"
                + "  ) {\n"
                + "    values {\n"
                + "      id, prodId, prodName, customerName, address,\n"
                + "      description, price, sellPrice\n"
                + "    }\n"
                + "  }\n"
                + "}");
    assertIsOrder(
        JsonPath.read(response, "$.Orders.values[0]"),
        "dd73afe2-9841-4ce1-b841-575b8be405c1",
        "31047029-2175-43ce-9fdd-b3d568b19bb5",
        "Basic Task CPU",
        "John Doe",
        "123 Main St 67890",
        "Ordering replacement CPUs.",
        "899.99",
        "900.82");
  }

  @Test
  public void getOrdersWithFilterAndLimit(@TestKeyspace CqlIdentifier keyspaceId) {
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "{\n"
                + "  Orders(\n"
                + "    filter: {\n"
                + "      prodName: { eq: \"Basic Task CPU\" }\n"
                + "      customerName: { eq: \"John Doe\" }\n"
                + "    },\n"
                + "    options: { limit: 1 }"
                + "  ) {\n"
                + "    values {\n"
                + "      id, prodId, prodName, customerName, address,\n"
                + "      description, price, sellPrice\n"
                + "    }\n"
                + "  }\n"
                + "}");
    assertIsOrder(
        JsonPath.read(response, "$.Orders.values[0]"),
        "dd73afe2-9841-4ce1-b841-575b8be405c1",
        "31047029-2175-43ce-9fdd-b3d568b19bb5",
        "Basic Task CPU",
        "John Doe",
        "123 Main St 67890",
        "Ordering replacement CPUs.",
        "899.99",
        "900.82");
  }
}
