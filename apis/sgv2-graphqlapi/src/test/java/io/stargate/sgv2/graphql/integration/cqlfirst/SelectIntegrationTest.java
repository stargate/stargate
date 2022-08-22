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

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.BetterBotzIntegrationTestBase;
import java.util.Map;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SelectIntegrationTest extends BetterBotzIntegrationTestBase {

  @Test
  public void getOrdersByValue() {
    Map<String, Object> response =
        client.executeDmlQuery(
            keyspaceId.asInternal(),
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
  public void getOrdersWithFilter() {
    Map<String, Object> response =
        client.executeDmlQuery(
            keyspaceId.asInternal(),
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
  public void getOrdersWithFilterAndLimit() {
    Map<String, Object> response =
        client.executeDmlQuery(
            keyspaceId.asInternal(),
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
