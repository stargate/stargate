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
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE data(id int, x1 text, x2 text, value int, PRIMARY KEY ((id), x1));",
      "INSERT INTO data(id, x1, x2, value) values (1, 'a', 'b', 20);",
      "INSERT INTO data(id, x1, x2, value) values (1, 'b', 'b', 30);",
      "INSERT INTO data(id, x1, x2, value) values (2, 'a', 'b', 40);",
      "INSERT INTO data(id, x1, x2, value) values (3, 'a', 'b', 50);",
      "INSERT INTO data(id, x1, x2, value) values (4, 'a', 'b', 60);",
    })
public class EnrichedQueryTest extends BridgeIntegrationTest {

  @Test
  public void enriched(@TestKeyspace CqlIdentifier keyspace) {
    StargateBridgeBlockingStub stub = stubWithCallCredentials();

    QueryOuterClass.Response response =
        stub.executeQuery(
            cqlQuery("SELECT * FROM data;", queryParameters(keyspace).setEnriched(true)));

    QueryOuterClass.ResultSet rs = response.getResultSet();
    assertThat(rs.getRowsList())
        .hasSize(5)
        .allSatisfy(
            r -> {
              assertThat(r.hasComparableBytes()).isTrue();
              assertThat(r.hasPagingState()).isFalse();
              assertThat(r.getValuesCount()).isEqualTo(4);
            });
  }

  @Test
  public void enrichedNextRow(@TestKeyspace CqlIdentifier keyspace) {
    StargateBridgeBlockingStub stub = stubWithCallCredentials();

    QueryOuterClass.ResumeModeValue.Builder resumeMode =
        QueryOuterClass.ResumeModeValue.newBuilder().setValue(QueryOuterClass.ResumeMode.NEXT_ROW);
    QueryOuterClass.Response response =
        stub.executeQuery(
            cqlQuery(
                "SELECT * FROM data;",
                queryParameters(keyspace).setEnriched(true).setResumeMode(resumeMode)));

    QueryOuterClass.ResultSet rs = response.getResultSet();
    assertThat(rs.getRowsList()).hasSize(5);
    assertThat(rs.getRowsList().subList(0, 4))
        .allSatisfy(
            r -> {
              assertThat(r.hasComparableBytes()).isTrue();
              assertThat(r.hasPagingState()).isTrue();
              assertThat(r.getPagingState().toByteString().toByteArray()).isNotEmpty();
              assertThat(r.getValuesCount()).isEqualTo(4);
            });
    assertThat(rs.getRowsList().get(4))
        .satisfies(
            r -> {
              assertThat(r.hasComparableBytes()).isTrue();
              assertThat(r.hasPagingState()).isTrue();
              assertThat(r.getPagingState().toByteString().toByteArray()).isEmpty();
              assertThat(r.getValuesCount()).isEqualTo(4);
            });

    // let's get first row paging state
    BytesValue firstRow = rs.getRows(0).getPagingState();
    QueryOuterClass.Response response2 =
        stub.executeQuery(
            cqlQuery("SELECT * FROM data;", queryParameters(keyspace).setPagingState(firstRow)));

    // as it's next row it should have 4 elements
    QueryOuterClass.ResultSet rs2 = response2.getResultSet();
    int valueIndex = indexOf(rs.getColumnsList(), "value");
    assertThat(rs2.getRowsList())
        .hasSize(4)
        .extracting(r -> r.getValues(valueIndex).getInt())
        .contains(30L, 40L, 50L, 60L);
  }

  @Test
  public void enrichedNextPartition(@TestKeyspace CqlIdentifier keyspace) {
    StargateBridgeBlockingStub stub = stubWithCallCredentials();

    QueryOuterClass.ResumeModeValue.Builder resumeMode =
        QueryOuterClass.ResumeModeValue.newBuilder()
            .setValue(QueryOuterClass.ResumeMode.NEXT_PARTITION);
    QueryOuterClass.Response response =
        stub.executeQuery(
            cqlQuery(
                "SELECT * FROM data;",
                queryParameters(keyspace).setEnriched(true).setResumeMode(resumeMode)));

    QueryOuterClass.ResultSet rs = response.getResultSet();
    assertThat(rs.getRowsList()).hasSize(5);
    assertThat(rs.getRowsList().subList(0, 4))
        .allSatisfy(
            r -> {
              assertThat(r.hasComparableBytes()).isTrue();
              assertThat(r.hasPagingState()).isTrue();
              assertThat(r.getPagingState().toByteString().toByteArray()).isNotEmpty();
              assertThat(r.getValuesCount()).isEqualTo(4);
            });
    assertThat(rs.getRowsList().get(4))
        .satisfies(
            r -> {
              assertThat(r.hasComparableBytes()).isTrue();
              assertThat(r.hasPagingState()).isTrue();
              assertThat(r.getPagingState().toByteString().toByteArray()).isEmpty();
              assertThat(r.getValuesCount()).isEqualTo(4);
            });

    // let's get first row paging state
    BytesValue firstRow = rs.getRows(0).getPagingState();
    QueryOuterClass.Response response2 =
        stub.executeQuery(
            cqlQuery("SELECT * FROM data;", queryParameters(keyspace).setPagingState(firstRow)));

    // as it's next partition it should skip one row in same partition
    QueryOuterClass.ResultSet rs2 = response2.getResultSet();
    int idIndex = indexOf(rs.getColumnsList(), "id");
    assertThat(rs2.getRowsList())
        .hasSize(3)
        .extracting(r -> r.getValues(idIndex).getInt())
        .contains(2L, 3L, 4L);
  }

  @Test
  public void notEnriched(@TestKeyspace CqlIdentifier keyspace) {
    StargateBridgeBlockingStub stub = stubWithCallCredentials();

    QueryOuterClass.Response response =
        stub.executeQuery(
            cqlQuery("SELECT * FROM data;", queryParameters(keyspace).setEnriched(false)));

    assertThat(response).isNotNull();

    QueryOuterClass.ResultSet rs = response.getResultSet();
    assertThat(rs.getRowsList())
        .hasSize(5)
        .allSatisfy(
            r -> {
              assertThat(r.hasComparableBytes()).isFalse();
              assertThat(r.hasPagingState()).isFalse();
              assertThat(r.getValuesCount()).isEqualTo(4);
            });
  }

  private int indexOf(List<QueryOuterClass.ColumnSpec> columnsList, String column) {
    for (int i = 0; i < columnsList.size(); i++) {
      if (Objects.equals(column, columnsList.get(i).getName())) {
        return i;
      }
    }
    throw new IllegalArgumentException(
        String.format("Can not find column %s in the column list %s", column, columnsList));
  }
}
