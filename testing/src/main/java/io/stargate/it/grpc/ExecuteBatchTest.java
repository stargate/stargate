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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.protobuf.InvalidProtocolBufferException;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
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
      "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))",
    })
public class ExecuteBatchTest extends GrpcIntegrationTest {

  @Test
  public void simpleBatch(@TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeBatch(
            Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
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
                    rowOf(Values.of("a"), Values.of(1)),
                    rowOf(Values.of("b"), Values.of(2)),
                    rowOf(Values.of("c"), Values.of(3)))));
  }

  @Test
  public void simpleBatchAfterSchemaChange() throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();
    stub.executeQuery(cqlQuery("DROP KEYSPACE IF EXISTS ks1"));

    // Create keyspace, table, and then insert some data
    Response response =
        stub.executeQuery(
            cqlQuery(
                "CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};"));
    assertThat(response).isNotNull();

    response =
        stub.executeQuery(
            cqlQuery("CREATE TABLE IF NOT EXISTS ks1.tbl1 (k text, v int, PRIMARY KEY (k));"));
    assertThat(response).isNotNull();

    response =
        stub.executeBatch(
            Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO ks1.tbl1 (k, v) VALUES ('a', 1)"))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO ks1.tbl1 (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO ks1.tbl1 (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
                .build());
    assertThat(response).isNotNull();

    response = stub.executeQuery(cqlQuery("SELECT * FROM ks1.tbl1"));
    assertThat(response.hasResultSet()).isTrue();
    ResultSet rs = response.getResultSet();
    assertThat(new HashSet<>(rs.getRowsList()))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    rowOf(Values.of("a"), Values.of(1)),
                    rowOf(Values.of("b"), Values.of(2)),
                    rowOf(Values.of("c"), Values.of(3)))));

    // Drop the keyspace to cause the existing prepared queries to be purged from the backend query
    // cache
    response = stub.executeQuery(cqlQuery("DROP KEYSPACE ks1;"));
    assertThat(response).isNotNull();

    response =
        stub.executeQuery(
            cqlQuery(
                "CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};"));
    assertThat(response).isNotNull();

    response =
        stub.executeQuery(
            cqlQuery("CREATE TABLE IF NOT EXISTS ks1.tbl1 (k text, v int, PRIMARY KEY (k));"));
    assertThat(response).isNotNull();

    response =
        stub.executeBatch(
            Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO ks1.tbl1 (k, v) VALUES ('a', 1)"))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO ks1.tbl1 (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO ks1.tbl1 (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
                .build());
    assertThat(response).isNotNull();
  }
}
