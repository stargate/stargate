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
package io.stargate.it.bridge;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.protobuf.BytesValue;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.BridgeQuery;
import io.stargate.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE data(id int PRIMARY KEY, x1 text, x2 text, value int);",
      "INSERT INTO data(id, x1, x2, value) values (1, 'a', 'b', 20);",
      "INSERT INTO data(id, x1, x2, value) values (2, 'a', 'b', 30);",
      "INSERT INTO data(id, x1, x2, value) values (3, 'a', 'b', 40);",
      "INSERT INTO data(id, x1, x2, value) values (4, 'a', 'b', 50);",
      "INSERT INTO data(id, x1, x2, value) values (5, 'a', 'b', 60);",
    })
public class EnrichedQueryTest extends BridgeIntegrationTest {

  @Test
  public void getEnrichedDataFromRows(@TestKeyspace CqlIdentifier keyspace) {
    StargateBridgeBlockingStub stub = stubWithCallCredentials();

    BridgeQuery.EnrichedResponse response =
        stub.executeEnrichedQuery(cqlQuery("SELECT * FROM data;", queryParameters(keyspace)));
    assertThat(response).isNotNull();
    BridgeQuery.EnrichedResultSet rs = response.getResultSet();
    assertThat(rs.getRowsCount()).isEqualTo(5);
    for (int i = 0; i < 5; i++) {
      BridgeQuery.EnrichedRow r = rs.getRows(i);
      assertThat(r.getComparableBytes()).isNotNull();
      assertThat(r.getPagingState()).isNotNull();
      assertThat(r.getRow().getValuesCount()).isEqualTo(4);
    }
  }

  @Test
  public void getEnrichedDataUsingPagination(@TestKeyspace CqlIdentifier keyspace) {
    StargateBridgeBlockingStub stub = stubWithCallCredentials();

    BridgeQuery.EnrichedResponse response =
        stub.executeEnrichedQuery(cqlQuery("SELECT * FROM data;", queryParameters(keyspace)));
    assertThat(response).isNotNull();
    BridgeQuery.EnrichedResultSet rs = response.getResultSet();
    assertThat(rs.getRowsCount()).isEqualTo(5);
    BytesValue thirdRow = rs.getRows(2).getPagingState();

    BridgeQuery.EnrichedResponse response2 =
        stub.executeEnrichedQuery(
            cqlQuery("SELECT * FROM data;", queryParameters(keyspace).setPagingState(thirdRow)));

    BridgeQuery.EnrichedResultSet rs2 = response2.getResultSet();
    assertThat(rs2.getRowsCount()).isEqualTo(2);

    assertThat(rs2.getRows(0).getRow().getValues(0)).isEqualTo(rs.getRows(3).getRow().getValues(0));
    assertThat(rs2.getRows(1).getRow().getValues(0)).isEqualTo(rs.getRows(4).getRow().getValues(0));
  }
}
