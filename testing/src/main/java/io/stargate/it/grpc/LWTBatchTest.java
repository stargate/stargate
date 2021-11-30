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

package io.stargate.it.grpc;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.protobuf.InvalidProtocolBufferException;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k text PRIMARY KEY, v int)",
    })
public class LWTBatchTest extends GrpcIntegrationTest {

  @Test
  public void simpleBatchLWT(@TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    // given normal batch inserts
    Response response =
        stub.executeBatch(
            Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
                .setParameters(batchParameters(keyspace))
                .build());
    assertThat(response).isNotNull();

    response = stub.executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace)));
    assertThat(response.hasResultSet()).isTrue();
    ResultSet rs = response.getResultSet();
    assertThat(new HashSet<>(rs.getRowsList()))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    rowOf(Values.of("a"), Values.of(1)), rowOf(Values.of("b"), Values.of(2)))));

    // when batch insert with LWTs
    response =
        stub.executeBatch(
            Batch.newBuilder()
                .addQueries(
                    cqlBatchQuery("UPDATE test set v = 2 where k = 'a' IF v = 1")) // successful LWT
                .addQueries(
                    cqlBatchQuery("UPDATE test set v = 2 where k = 'a' IF v = 100")) // failed LWT
                .setParameters(batchParameters(keyspace))
                .build());

    // then response should contain one record
    assertThat(response).isNotNull();
    ResultSet resultSet = response.getResultSet();
    QueryOuterClass.Row row = resultSet.getRows(0);
    boolean applied = row.getValues(0).getBoolean();
    assertThat(applied).isFalse();
    String k = row.getValues(1).getString();
    assertThat(k).isEqualTo("a");
    long v = row.getValues(2).getInt();
    assertThat(v).isEqualTo(1L);
  }
}
