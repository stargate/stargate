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

import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for GraphQL CQL-first tests that share the data model below. */
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS \"Products\" (\n"
          + "    id uuid,\n"
          + "    name text,\n"
          + "    price decimal,\n"
          + "    created timestamp,\n"
          + "    \"prodName\" text,\n"
          + "    \"customerName\" text,\n"
          + "    description text,\n"
          + "    PRIMARY KEY ((id), name, price, created)\n"
          + ")",
      "CREATE TABLE IF NOT EXISTS \"Orders\" (\n"
          + "    \"prodName\" text,\n"
          + "    \"customerName\" text,\n"
          + "    id uuid,\n"
          + "    \"prodId\" uuid,\n"
          + "    address text,\n"
          + "    description text,\n"
          + "    price decimal,\n"
          + "    \"sellPrice\" decimal,\n"
          + "    PRIMARY KEY ((\"prodName\"), \"customerName\")\n"
          + ")",
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
          + "'2101 Everplace Ave 3116')",
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
          + "'123 Main St 67890')",
    })
abstract class BetterbotzTestBase extends BaseOsgiIntegrationTest {}
