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
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(initQueries = {"CREATE TABLE foo(k int, cc int, v int, PRIMARY KEY (k, cc))"})
public class AtomicBulkInsertTest extends BaseIntegrationTest {

  private static CqlFirstClient CLIENT;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) {
    String host = cluster.seedAddress();
    CLIENT = new CqlFirstClient(host, RestUtils.getAuthToken(host));
  }

  @Test
  @DisplayName("Should generate CQL batch for atomic bulk insert with multiple values")
  public void atomicBulkInsert(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  bulkInsertfoo(values: [{ k: 1, cc: 1, v: 1 }, { k: 1, cc: 2, v: 2 }]) {\n"
            + "    applied\n"
            + "  }\n"
            + "}";

    // When
    CLIENT.executeDmlQuery(keyspaceId, query);

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
