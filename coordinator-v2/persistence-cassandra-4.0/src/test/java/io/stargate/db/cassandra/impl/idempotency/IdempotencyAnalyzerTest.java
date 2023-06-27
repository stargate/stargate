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
package io.stargate.db.cassandra.impl.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import io.stargate.auth.AuthorizationService;
import io.stargate.db.cassandra.impl.BaseCassandraTest;
import io.stargate.db.cassandra.impl.StargateQueryHandler;
import jakarta.enterprise.inject.Instance;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IdempotencyAnalyzerTest extends BaseCassandraTest {

  StargateQueryHandler queryHandler;

  @Mock(strictness = Mock.Strictness.LENIENT)
  AuthorizationService authorizationService;

  @Mock(strictness = Mock.Strictness.LENIENT)
  Instance<AuthorizationService> authorizationServiceInstance;

  @BeforeEach
  public void initTest() {
    when(authorizationServiceInstance.isResolvable()).thenReturn(true);
    when(authorizationServiceInstance.get()).thenReturn(authorizationService);

    queryHandler = new StargateQueryHandler();
    queryHandler.setAuthorizationService(authorizationServiceInstance);

    TableMetadata tableMetadata =
        TableMetadata.builder("ks_idempotency", "my_table")
            .addPartitionKeyColumn("pk", IntegerType.instance)
            .addRegularColumn("value", AsciiType.instance)
            .addRegularColumn("v", UUIDType.instance)
            .addRegularColumn("v_lwt", IntegerType.instance)
            .addRegularColumn("list_col", ListType.getInstance(IntegerType.instance, true))
            .addRegularColumn("counter_value", CounterColumnType.instance)
            .addRegularColumn(
                "map", MapType.getInstance(AsciiType.instance, AsciiType.instance, true))
            .addRegularColumn("set_c", SetType.getInstance(AsciiType.instance, true))
            .build();

    KeyspaceMetadata keyspaceMetadata =
        KeyspaceMetadata.create("ks_idempotency", KeyspaceParams.local(), Tables.of(tableMetadata));
    if (Schema.instance.getKeyspaceMetadata("ks_idempotency") == null) {
      Schema.instance.load(keyspaceMetadata);
    }

    CommitLog.instance.start();
  }

  @ParameterizedTest
  @MethodSource("queriesToInferIdempotence")
  public void validateIdempotencyOfQueries(String cqlQuery, boolean isIdempotent) {
    CQLStatement.Raw raw = QueryProcessor.parseStatement(cqlQuery);

    CQLStatement statement = raw.prepare(ClientState.forInternalCalls());

    assertThat(IdempotencyAnalyzer.isIdempotent(statement)).isEqualTo(isIdempotent);
  }

  @Test
  public void shouldReturnIdempotentIfAllStatementsWithinABatchAreIdempotent() {
    BatchStatement.Parsed raw =
        (BatchStatement.Parsed)
            QueryProcessor.parseStatement(
                "BEGIN BATCH\n"
                    + "update ks_idempotency.my_table SET list_col = [1] WHERE pk = 1\n"
                    + "UPDATE ks_idempotency.my_table SET map['key'] = 'V' WHERE pk = 123\n"
                    + "APPLY BATCH;");

    CQLStatement statement = raw.prepare(ClientState.forInternalCalls());

    assertThat(IdempotencyAnalyzer.isIdempotent(statement)).isTrue();
  }

  @Test
  public void shouldReturnNonIdempotentIfAllStatementsWithinABatchAreNonIdempotent() {
    BatchStatement.Parsed raw =
        (BatchStatement.Parsed)
            QueryProcessor.parseStatement(
                "BEGIN BATCH\n"
                    + "update ks_idempotency.my_table SET list_col = [1] WHERE pk = 1\n"
                    + "DELETE list_col[1] FROM ks_idempotency.my_table WHERE pk = 1\n"
                    + "APPLY BATCH;");

    CQLStatement statement = raw.prepare(ClientState.forInternalCalls());

    assertThat(IdempotencyAnalyzer.isIdempotent(statement)).isFalse();
  }

  public static Stream<Arguments> queriesToInferIdempotence() {
    return Stream.of(
        arguments(
            "update ks_idempotency.my_table SET list_col = [1] WHERE pk = 1", true), // collection
        arguments(
            "UPDATE ks_idempotency.my_table SET list_col = [1] + list_col WHERE pk = 1",
            false), // append to list
        arguments(
            "UPDATE ks_idempotency.my_table SET list_col = list_col + [1] where pk = 1",
            false), // prepend to list
        arguments(
            "DELETE list_col[1] FROM ks_idempotency.my_table WHERE pk = 1",
            false), // delete from list
        arguments(
            "UPDATE ks_idempotency.my_table SET v = now() WHERE pk = 1",
            false), // using now() function
        arguments(
            "UPDATE ks_idempotency.my_table SET v = uuid() WHERE pk = 1",
            false), // using uuid() function
        arguments(
            "UPDATE ks_idempotency.my_table SET counter_value = counter_value + 1 WHERE pk = 1",
            false), // counter
        arguments(
            "UPDATE ks_idempotency.my_table SET v_lwt = 4 WHERE pk = 1 IF v_lwt = 1",
            false), // transaction
        arguments(
            "UPDATE ks_idempotency.my_table SET v_lwt = 4 WHERE pk = 1 if EXISTS",
            false), // transaction
        arguments(
            "UPDATE ks_idempotency.my_table SET map['key'] = 'V' WHERE pk = 123",
            true), // update map
        arguments(
            "DELETE map ['v'] FROM ks_idempotency.my_table WHERE pk = 123",
            true), // delete from map
        arguments(
            "UPDATE ks_idempotency.my_table SET set_c = set_c + {'T'} WHERE pk = 123",
            true), // add to set
        arguments(
            "UPDATE ks_idempotency.my_table SET set_c = set_c - { 'Banana'} WHERE pk = 7801;",
            true), // remove from set
        arguments(
            "DELETE set_c FROM ks_idempotency.my_table WHERE pk = 123",
            true), // delete all from a set
        arguments(
            "UPDATE ks_idempotency.my_table SET value = 'M' WHERE pk = 123",
            true), // standard update
        arguments(
            "INSERT INTO ks_idempotency.my_table (pk, value) VALUES (123, 'aaa');", true), // insert
        arguments("TRUNCATE ks_idempotency.my_table", false), // truncate
        arguments("ALTER TABLE ks_idempotency.my_table DROP v", false), // alter schema
        arguments("USE ks_idempotency", false) // USE
        );
  }
}
