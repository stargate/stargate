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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.ApiServiceConnectionInfo;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.graphql.BaseGraphqlV2ApiTest;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(initQueries = {"CREATE TABLE foo(k int, cc int, v int, PRIMARY KEY (k, cc))"})
public class AtomicBatchTest extends BaseGraphqlV2ApiTest {

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

  @BeforeEach
  public void cleanup(CqlSession session) {
    session.execute("TRUNCATE TABLE foo");
    session.execute("INSERT INTO foo (k, cc, v) VALUES (1, 1, 1)");
    session.execute("INSERT INTO foo (k, cc, v) VALUES (1, 2, 2)");
  }

  @Test
  @DisplayName("Should handle failed batch with conditional updates")
  public void failedConditionalBatch(@TestKeyspace CqlIdentifier keyspaceId) {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  update1: updatefoo(value: { k: 1, cc: 1, v: 3 }, ifCondition: { v: { eq: 0 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "  update2: updatefoo(value: { k: 1, cc: 2, v: 3 }, ifCondition: { v: { eq: 0 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "}";

    // When
    Map<String, Object> response = CLIENT.executeDmlQuery(keyspaceId, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.update1.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.v")).isEqualTo(1);

    assertThat(JsonPath.<Boolean>read(response, "$.update2.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.cc")).isEqualTo(2);
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.v")).isEqualTo(2);
  }

  @Test
  @DisplayName("Should handle failed batch with conditional updates not in PK order")
  public void failedConditionalBatchOutOfOrder(@TestKeyspace CqlIdentifier keyspaceId) {
    // Given
    // Queries in reverse PK order. This doesn't change anything from a user POV, but internally the
    // rows are always returned in PK order, so make sure we deal with that correctly.
    String query =
        "mutation @atomic {\n"
            + "  update2: updatefoo(value: { k: 1, cc: 2, v: 3 }, ifCondition: { v: { eq: 0 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "  update1: updatefoo(value: { k: 1, cc: 1, v: 3 }, ifCondition: { v: { eq: 0 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "}";

    // When
    Map<String, Object> response = CLIENT.executeDmlQuery(keyspaceId, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.update1.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.v")).isEqualTo(1);

    assertThat(JsonPath.<Boolean>read(response, "$.update2.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.cc")).isEqualTo(2);
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.v")).isEqualTo(2);
  }

  @Test
  @DisplayName("Should handle failed batch mixing conditional and regular updates")
  public void failedMixedBatch(@TestKeyspace CqlIdentifier keyspaceId) {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  update1: updatefoo(value: { k: 1, cc: 1, v: 3 } ) {\n" // not conditional
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "  update2: updatefoo(value: { k: 1, cc: 2, v: 3 }, ifCondition: { v: { eq: 0 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "}";

    // When
    Map<String, Object> response = CLIENT.executeDmlQuery(keyspaceId, query);

    // Then
    // None of the queries are applied. For the regular one, we don't have any data to echo back:
    assertThat(JsonPath.<Boolean>read(response, "$.update1.applied")).isFalse();
    assertThat(JsonPath.<Object>read(response, "$.update1.value")).isNull();

    assertThat(JsonPath.<Boolean>read(response, "$.update2.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.cc")).isEqualTo(2);
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.v")).isEqualTo(2);
  }

  @Test
  @DisplayName("Should handle failed batch with multiple conditional updates on the same row")
  public void failedConditionalBatchDuplicateRows(@TestKeyspace CqlIdentifier keyspaceId) {
    // Given
    String query =
        "mutation @atomic {\n"
            // Unlikely to happen in real use cases, but CQL technically allows it, so we need to
            // handle it correctly (a single row gets returned).
            + "  update1: updatefoo(value: { k: 1, cc: 1, v: 3 }, ifCondition: { v: { eq: 0 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "  update2: updatefoo(value: { k: 1, cc: 1, v: 4 }, ifCondition: { v: { eq: -1 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "}";

    // When
    Map<String, Object> response = CLIENT.executeDmlQuery(keyspaceId, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.update1.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.v")).isEqualTo(1);

    assertThat(JsonPath.<Boolean>read(response, "$.update2.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.v")).isEqualTo(1);
  }

  @Test
  @DisplayName("Should handle successful conditional batch")
  public void successfulConditionalBatch(@TestKeyspace CqlIdentifier keyspaceId) {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  update1: updatefoo(value: { k: 1, cc: 1, v: 3 }, ifCondition: { v: { eq: 1 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "  update2: updatefoo(value: { k: 1, cc: 2, v: 4 }, ifCondition: { v: { eq: 2 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "}";

    // When
    Map<String, Object> response = CLIENT.executeDmlQuery(keyspaceId, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.update1.applied")).isTrue();
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update1.value.v")).isEqualTo(3);

    assertThat(JsonPath.<Boolean>read(response, "$.update2.applied")).isTrue();
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.cc")).isEqualTo(2);
    assertThat(JsonPath.<Integer>read(response, "$.update2.value.v")).isEqualTo(4);
  }

  @Test
  @DisplayName("Should handle failed conditional batch with bulk insert")
  public void failedConditionalBatchWithBulkInsert(@TestKeyspace CqlIdentifier keyspaceId) {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  updatefoo(value: { k: 1, cc: 1, v: 3 }, ifCondition: { v: { eq: 3 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "  bulkInsertfoo(values: [\n"
            + "                  { k: 1, cc: 2, v: 3 },\n"
            + "                  { k: 1, cc: 3, v: 3 }\n"
            + "                ],\n"
            + "                ifNotExists: true) {\n"
            + "    applied, value {k, cc, v}\n"
            + "  }\n"
            + "}";

    // When
    Map<String, Object> response = CLIENT.executeDmlQuery(keyspaceId, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.updatefoo.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.updatefoo.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.updatefoo.value.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.updatefoo.value.v")).isEqualTo(1);

    assertThat(JsonPath.<Boolean>read(response, "$.bulkInsertfoo[0].applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.bulkInsertfoo[0].value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.bulkInsertfoo[0].value.cc")).isEqualTo(2);
    assertThat(JsonPath.<Integer>read(response, "$.bulkInsertfoo[0].value.v")).isEqualTo(2);

    assertThat(JsonPath.<Boolean>read(response, "$.bulkInsertfoo[1].applied")).isFalse();
    assertThat(JsonPath.<Object>read(response, "$.bulkInsertfoo[1].value")).isNull();
  }

  @Test
  @DisplayName("Should handle successful conditional batch with bulk insert")
  public void successfulConditionalBatchWithBulkInsert(@TestKeyspace CqlIdentifier keyspaceId) {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  updatefoo(value: { k: 1, cc: 1, v: 3 }, ifCondition: { v: { eq: 1 } }) {\n"
            + "    applied, value { k, cc, v}\n"
            + "  }\n"
            + "  bulkInsertfoo(values: [\n"
            + "                  { k: 1, cc: 3, v: 3 },\n"
            + "                  { k: 1, cc: 4, v: 4 }\n"
            + "                ],\n"
            + "                ifNotExists: true) {\n"
            + "    applied, value {k, cc, v}\n"
            + "  }\n"
            + "}";

    // When
    Map<String, Object> response = CLIENT.executeDmlQuery(keyspaceId, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.updatefoo.applied")).isTrue();
    assertThat(JsonPath.<Integer>read(response, "$.updatefoo.value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.updatefoo.value.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.updatefoo.value.v")).isEqualTo(3);

    assertThat(JsonPath.<Boolean>read(response, "$.bulkInsertfoo[0].applied")).isTrue();
    assertThat(JsonPath.<Integer>read(response, "$.bulkInsertfoo[0].value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.bulkInsertfoo[0].value.cc")).isEqualTo(3);
    assertThat(JsonPath.<Integer>read(response, "$.bulkInsertfoo[0].value.v")).isEqualTo(3);

    assertThat(JsonPath.<Boolean>read(response, "$.bulkInsertfoo[1].applied")).isTrue();
    assertThat(JsonPath.<Integer>read(response, "$.bulkInsertfoo[1].value.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.bulkInsertfoo[1].value.cc")).isEqualTo(4);
    assertThat(JsonPath.<Integer>read(response, "$.bulkInsertfoo[1].value.v")).isEqualTo(4);
  }
}
