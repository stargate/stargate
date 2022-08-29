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

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.CqlFirstIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AtomicBulkInsertIntegrationTest extends CqlFirstIntegrationTest {

  @BeforeAll
  public void createSchema() {
    session.execute("CREATE TABLE foo(k int, cc int, v int, PRIMARY KEY (k, cc))");
  }

  @Test
  @DisplayName("Should generate CQL batch for atomic bulk insert with multiple values")
  public void atomicBulkInsert() {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  bulkInsertfoo(values: [{ k: 1, cc: 1, v: 1 }, { k: 1, cc: 2, v: 2 }]) {\n"
            + "    applied\n"
            + "  }\n"
            + "}";

    // When
    client.executeDmlQuery(keyspaceId.asInternal(), query);

    // Then
    // The write times can only be equal if the CQL queries have been executed atomically.
    // Otherwise, Cassandra will always order one before the other.
    long writetime1 =
        session.execute("SELECT writetime(v) FROM foo WHERE k = 1 and cc = 1").one().getLong(0);
    long writetime2 =
        session.execute("SELECT writetime(v) FROM foo WHERE k = 1 and cc = 2").one().getLong(0);
    assertThat(writetime1).isEqualTo(writetime2);
  }
}
