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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.SchemaChange;
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
public class ExecuteQueryTest extends GrpcIntegrationTest {

  @Test
  public void simpleQuery(@TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeQuery(
            cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace)));
    assertThat(response).isNotNull();
    response =
        stub.executeQuery(
            cqlQuery(
                "INSERT INTO test (k, v) VALUES (?, ?)",
                queryParameters(keyspace),
                Values.of("b"),
                Values.of(2)));
    assertThat(response).isNotNull();

    response = stub.executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace)));
    assertThat(response.hasResultSet()).isTrue();
    assertThat(response.getResultSet().getType()).isEqualTo(Payload.Type.CQL);
    ResultSet rs = response.getResultSet().getData().unpack(ResultSet.class);
    assertThat(new HashSet<>(rs.getRowsList()))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    cqlRow(Values.of("a"), Values.of(1)), cqlRow(Values.of("b"), Values.of(2)))));
  }

  @Test
  public void queryAfterSchemaChange() {
    StargateBlockingStub stub = stubWithCallCredentials();
    stub.executeQuery(cqlQuery("DROP KEYSPACE IF EXISTS ks1"));

    // Create keyspace, table, and then insert some data
    Response response =
        stub.executeQuery(
            cqlQuery(
                "CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};"));
    assertThat(response).isNotNull();
    assertThat(response.hasSchemaChange()).isTrue();
    assertThat(response.getSchemaChange())
        .satisfies(
            c -> {
              assertThat(c.getChangeType()).isEqualTo(SchemaChange.Type.CREATED);
              assertThat(c.getTarget()).isEqualTo(SchemaChange.Target.KEYSPACE);
              assertThat(c.getKeyspace()).isEqualTo("ks1");
              assertThat(c.hasName()).isFalse();
              assertThat(c.getArgumentTypesCount()).isEqualTo(0);
            });

    response =
        stub.executeQuery(
            cqlQuery("CREATE TABLE IF NOT EXISTS ks1.tbl1 (k text, v int, PRIMARY KEY (k));"));
    assertThat(response).isNotNull();
    assertThat(response.hasSchemaChange()).isTrue();
    assertThat(response.getSchemaChange())
        .satisfies(
            c -> {
              assertThat(c.getChangeType()).isEqualTo(SchemaChange.Type.CREATED);
              assertThat(c.getTarget()).isEqualTo(SchemaChange.Target.TABLE);
              assertThat(c.getKeyspace()).isEqualTo("ks1");
              assertThat(c.getName().getValue()).isEqualTo("tbl1");
              assertThat(c.getArgumentTypesCount()).isEqualTo(0);
            });

    response = stub.executeQuery(cqlQuery("INSERT INTO ks1.tbl1 (k, v) VALUES ('a', 1)"));
    assertThat(response).isNotNull();
    assertThat(response.hasSchemaChange()).isFalse();

    // Drop the keyspace to cause the existing prepared queries to be purged from the backend query
    // cache
    response = stub.executeQuery(cqlQuery("DROP KEYSPACE ks1;"));
    assertThat(response).isNotNull();
    assertThat(response.hasSchemaChange()).isTrue();
    assertThat(response.getSchemaChange())
        .satisfies(
            c -> {
              assertThat(c.getChangeType()).isEqualTo(SchemaChange.Type.DROPPED);
              assertThat(c.getTarget()).isEqualTo(SchemaChange.Target.KEYSPACE);
              assertThat(c.getKeyspace()).isEqualTo("ks1");
              assertThat(c.hasName()).isFalse();
              assertThat(c.getArgumentTypesCount()).isEqualTo(0);
            });

    response =
        stub.executeQuery(
            cqlQuery(
                "CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};"));
    assertThat(response).isNotNull();

    response =
        stub.executeQuery(
            cqlQuery("CREATE TABLE IF NOT EXISTS ks1.tbl1 (k text, v int, PRIMARY KEY (k));"));
    assertThat(response).isNotNull();

    response = stub.executeQuery(cqlQuery("INSERT INTO ks1.tbl1 (k, v) VALUES ('a', 1)"));
    assertThat(response).isNotNull();
  }

  @Test
  public void simpleQueryWithPaging() throws InvalidProtocolBufferException {
    Response response =
        stubWithCallCredentials()
            .executeQuery(
                Query.newBuilder()
                    .setCql("select keyspace_name,table_name from system_schema.tables")
                    .setParameters(
                        QueryParameters.newBuilder()
                            .setPageSize(Int32Value.newBuilder().setValue(2).build())
                            .build())
                    .build());

    assertThat(response.hasResultSet()).isTrue();
    assertThat(response.getResultSet().getType()).isEqualTo(Payload.Type.CQL);
    ResultSet rs = response.getResultSet().getData().unpack(ResultSet.class);
    assertThat(rs.getRowsCount()).isEqualTo(2);
    assertThat(rs.getPagingState()).isNotNull();
  }

  @Test
  public void useKeyspace(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();
    assertThatThrownBy(
            () -> {
              Response response =
                  stub.executeQuery(Query.newBuilder().setCql("USE system").build());
              assertThat(response).isNotNull();
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("USE <keyspace> not supported");

    // Verify that system local doesn't work
    assertThatThrownBy(
            () -> {
              Response response =
                  stub.executeQuery(Query.newBuilder().setCql("SELECT * FROM local").build());
              assertThat(response).isNotNull();
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("No keyspace has been specified");

    // Verify that setting the keyspace using parameters still works
    Response response =
        stub.executeQuery(
            cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace)));
    assertThat(response).isNotNull();
  }
}
