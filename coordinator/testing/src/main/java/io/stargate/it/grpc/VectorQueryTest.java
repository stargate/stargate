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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.storage.ClusterConnectionInfo;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec
public class VectorQueryTest extends GrpcIntegrationTest {

    @BeforeAll
    public static void validateRunningOnVSearchEnabled(ClusterConnectionInfo backend) {
        assumeTrue(
                backend.supportsVSearch(),
                "Test disabled if backend does not support Vector Search (vsearch)");
    }

    @BeforeEach
    public void init(CqlSession session) {
        session.execute("DROP TABLE IF EXISTS vector_table");
        session.execute("CREATE TABLE IF NOT EXISTS vector_table (id int PRIMARY KEY, embedding vector<float, 5>)");
        session.execute("CREATE CUSTOM INDEX embedding_index ON vector_table(embedding) USING 'StorageAttachedIndex'");
    }

    @Test
    @DisplayName("Should be able to insert and fetch a vector value")
    @Order(1)
    public void insertAndGet(@TestKeyspace CqlIdentifier keyspace) {
        // Insert rows into table
        StargateGrpc.StargateBlockingStub stub = stubWithCallCredentials();
        QueryOuterClass.Response response =
                stub.executeQuery(
                        cqlQuery("INSERT into vector_table (id, embedding) values (1, [1.0, 0.5, 0.75, 0.125, 0.25])", queryParameters(keyspace)));
        assertThat(response).isNotNull();

        response =
                stub.executeQuery(
                        cqlQuery(
                                "INSERT into vector_table (id, embedding) values (2, [1.0, 1.0, 1.0, 1.0, 1.0])",
                                queryParameters(keyspace)));
        assertThat(response).isNotNull();

        // Get rows with Vector values
        response = stub.executeQuery(cqlQuery("SELECT * FROM vector_table", queryParameters(keyspace)));
        assertThat(response.hasResultSet()).isTrue();
        assertThat(response.getResultSet().getRowsCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should be able to insert and fetch rows by vector similarity")
    @Order(2)
    public void insertAndSearch(@TestKeyspace CqlIdentifier keyspace) {
        // Insert rows into table
        StargateGrpc.StargateBlockingStub stub = stubWithCallCredentials();
        QueryOuterClass.Response response =
                stub.executeQuery(
                        cqlQuery("INSERT into vector_table (id, embedding) values (1, [1.0, 0.5, 0.75, 0.125, 0.25])", queryParameters(keyspace)));
        assertThat(response).isNotNull();

        response =
                stub.executeQuery(
                        cqlQuery(
                                "INSERT into vector_table (id, embedding) values (2, [1.0, 1.0, 1.0, 1.0, 1.0])",
                                queryParameters(keyspace)));
        assertThat(response).isNotNull();

        // vector search
        response = stub.executeQuery(cqlQuery("SELECT id FROM vector_table ORDER BY embedding ANN OF [1,1,1,1,1] LIMIT 10", queryParameters(keyspace)));
        assertThat(response.hasResultSet()).isTrue();
        assertThat(response.getResultSet().getRowsCount()).isEqualTo(2);
        assertThat(response.getResultSet().getRows(0).getValues(0).getInt()).isEqualTo(2);
        assertThat(response.getResultSet().getRows(1).getValues(0).getInt()).isEqualTo(1);
    }

}
