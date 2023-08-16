package io.stargate.db.cassandra.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.AuthenticatedUser;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.DropRoleStatement;
import org.apache.cassandra.cql3.statements.GrantRoleStatement;
import org.apache.cassandra.cql3.statements.ListPermissionsStatement;
import org.apache.cassandra.cql3.statements.ListRolesStatement;
import org.apache.cassandra.cql3.statements.PermissionsManagementStatement;
import org.apache.cassandra.cql3.statements.RevokeRoleStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.cql3.statements.schema.AlterKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.AlterTableStatement;
import org.apache.cassandra.cql3.statements.schema.AlterTypeStatement;
import org.apache.cassandra.cql3.statements.schema.AlterViewStatement;
import org.apache.cassandra.cql3.statements.schema.CreateAggregateStatement;
import org.apache.cassandra.cql3.statements.schema.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTriggerStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.schema.CreateViewStatement;
import org.apache.cassandra.cql3.statements.schema.DropAggregateStatement;
import org.apache.cassandra.cql3.statements.schema.DropFunctionStatement;
import org.apache.cassandra.cql3.statements.schema.DropIndexStatement;
import org.apache.cassandra.cql3.statements.schema.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.DropTableStatement;
import org.apache.cassandra.cql3.statements.schema.DropTableStatement.Raw;
import org.apache.cassandra.cql3.statements.schema.DropTriggerStatement;
import org.apache.cassandra.cql3.statements.schema.DropTypeStatement;
import org.apache.cassandra.cql3.statements.schema.DropViewStatement;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

class StargateQueryHandlerTest extends BaseCassandraTest {

  AuthenticatedUser authenticatedUser = AuthenticatedUser.of("username", "token");
  AuthenticationSubject authenticationSubject = AuthenticationSubject.of("token", "username");
  StargateQueryHandler queryHandler;
  AuthorizationService authorizationService;

  @BeforeEach
  public void initTest() {
    authorizationService = mock(AuthorizationService.class);
    AtomicReference<AuthorizationService> atomicReference = new AtomicReference<>();
    atomicReference.set(authorizationService);
    queryHandler = new StargateQueryHandler();
    queryHandler.setAuthorizationService(atomicReference);

    TableMetadata tableMetadata =
        TableMetadata.builder("ks1", "tbl1")
            .addPartitionKeyColumn("key", AsciiType.instance)
            .addRegularColumn("value", AsciiType.instance)
            .build();

    KeyspaceMetadata keyspaceMetadata =
        KeyspaceMetadata.create("ks1", KeyspaceParams.local(), Tables.of(tableMetadata));
    if (Schema.instance.getKeyspaceMetadata("ks1") == null) {
      Schema.instance.transform(schema -> schema.withAddedOrUpdated(keyspaceMetadata));
    }

    TableMetadata cyclingTableMetadata =
        TableMetadata.builder("cycling", "tbl1")
            .addPartitionKeyColumn("key", AsciiType.instance)
            .addRegularColumn("value", AsciiType.instance)
            .build();

    KeyspaceMetadata cyclingKeyspaceMetadata =
        KeyspaceMetadata.create("cycling", KeyspaceParams.local(), Tables.of(cyclingTableMetadata));
    if (Schema.instance.getKeyspaceMetadata("cycling") == null) {
      Schema.instance.transform(schema -> schema.withAddedOrUpdated(cyclingKeyspaceMetadata));
    }
    CommitLog.instance.start();
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenSelectStatement(SourceAPI sourceApi) throws UnauthorizedException {
    SelectStatement.Raw rawStatement = QueryProcessor.parseStatement("select * from system.local");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeDataRead(refEq(authenticationSubject), eq("system"), eq("local"), eq(expected));
  }

  @Test
  void authorizeByTokenSelectStatementMissingRoleName() {
    SelectStatement.Raw rawStatement = QueryProcessor.parseStatement("select * from system.local");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                queryHandler.authorizeByToken(
                    ImmutableMap.of("stargate.auth.subject.token", ByteBuffer.allocate(10)),
                    statement));

    assertThat(thrown.getMessage()).isEqualTo("token and roleName must be provided");
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenModificationStatementDelete(SourceAPI sourceApi)
      throws UnauthorizedException {
    DeleteStatement.Parsed rawStatement =
        (DeleteStatement.Parsed)
            QueryProcessor.parseStatement("delete from ks1.tbl1 where key = ?");

    CQLStatement statement =
        rawStatement.prepare(
            new VariableSpecifications(
                Collections.singletonList(new ColumnIdentifier("key", true))));

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeDataWrite(
            refEq(authenticationSubject), eq("ks1"), eq("tbl1"), eq(Scope.DELETE), eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenModificationStatementModify(SourceAPI sourceApi)
      throws UnauthorizedException {
    UpdateStatement.Parsed rawStatement =
        (UpdateStatement.Parsed)
            QueryProcessor.parseStatement("update ks1.tbl1 set value = 'a' where key = ?");

    CQLStatement statement =
        rawStatement.prepare(
            new VariableSpecifications(
                Collections.singletonList(new ColumnIdentifier("key", true))));

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeDataWrite(
            refEq(authenticationSubject), eq("ks1"), eq("tbl1"), eq(Scope.MODIFY), eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenTruncateStatement(SourceAPI sourceApi) throws UnauthorizedException {
    TruncateStatement.Raw rawStatement = QueryProcessor.parseStatement("truncate ks1.tbl1");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeDataWrite(
            refEq(authenticationSubject), eq("ks1"), eq("tbl1"), eq(Scope.TRUNCATE), eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementCreateTable(SourceAPI sourceApi)
      throws UnauthorizedException {
    CreateTableStatement.Raw rawStatement =
        (CreateTableStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE TABLE IF NOT EXISTS ks1.tbl2 (key uuid PRIMARY KEY,value text);");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq("tbl2"),
            eq(Scope.CREATE),
            eq(expected),
            eq(ResourceKind.TABLE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementDeleteTable(SourceAPI sourceApi)
      throws UnauthorizedException {
    DropTableStatement.Raw rawStatement =
        (Raw) QueryProcessor.parseStatement("drop table ks1.tbl1");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq("tbl1"),
            eq(Scope.DROP),
            eq(expected),
            eq(ResourceKind.TABLE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementAlterTable(SourceAPI sourceApi)
      throws UnauthorizedException {
    AlterTableStatement.Raw rawStatement =
        (AlterTableStatement.Raw)
            QueryProcessor.parseStatement("ALTER TABLE ks1.tbl1 ADD val2 INT");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq("tbl1"),
            eq(Scope.ALTER),
            eq(expected),
            eq(ResourceKind.TABLE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementCreateKeyspace(SourceAPI sourceApi)
      throws UnauthorizedException {
    CreateKeyspaceStatement.Raw rawStatement =
        (CreateKeyspaceStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE KEYSPACE ks2 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks2"),
            eq(null),
            eq(Scope.CREATE),
            eq(expected),
            eq(ResourceKind.KEYSPACE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementDeleteKeyspace(SourceAPI sourceApi)
      throws UnauthorizedException {
    DropKeyspaceStatement.Raw rawStatement =
        (DropKeyspaceStatement.Raw) QueryProcessor.parseStatement("drop keyspace ks1");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq(null),
            eq(Scope.DROP),
            eq(expected),
            eq(ResourceKind.KEYSPACE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementAlterKeyspace(SourceAPI sourceApi)
      throws UnauthorizedException {
    AlterKeyspaceStatement.Raw rawStatement =
        (AlterKeyspaceStatement.Raw)
            QueryProcessor.parseStatement(
                "ALTER KEYSPACE ks1 WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'SearchAnalytics' : 1 } AND DURABLE_WRITES = false;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq(null),
            eq(Scope.ALTER),
            eq(expected),
            eq(ResourceKind.KEYSPACE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementAlterType(SourceAPI sourceApi)
      throws UnauthorizedException {
    AlterTypeStatement.Raw rawStatement =
        (AlterTypeStatement.Raw)
            QueryProcessor.parseStatement("ALTER TYPE cycling.fullname ADD middlename text;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("cycling"),
            eq(null),
            eq(Scope.ALTER),
            eq(expected),
            eq(ResourceKind.TYPE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementAlterView(SourceAPI sourceApi)
      throws UnauthorizedException {
    AlterViewStatement.Raw rawStatement =
        (AlterViewStatement.Raw)
            QueryProcessor.parseStatement(
                "ALTER MATERIALIZED VIEW cycling.cyclist_by_age \n"
                    + "WITH compression = { \n"
                    + "  'sstable_compression' : 'DeflateCompressor', \n"
                    + "  'chunk_length_kb' : 64\n"
                    + "}\n"
                    + "AND compaction = {\n"
                    + "  'class' : 'SizeTieredCompactionStrategy', \n"
                    + "  'max_threshold' : 64\n"
                    + "};");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("cycling"),
            eq(null),
            eq(Scope.ALTER),
            eq(expected),
            eq(ResourceKind.VIEW));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementCreateAggregate(SourceAPI sourceApi)
      throws UnauthorizedException {
    CreateAggregateStatement.Raw rawStatement =
        (CreateAggregateStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE OR REPLACE AGGREGATE cycling.average (\n"
                    + "  int\n"
                    + ") \n"
                    + "  SFUNC avgState \n"
                    + "  STYPE tuple<int,bigint> \n"
                    + "  FINALFUNC avgFinal \n"
                    + "  INITCOND (0, 0)\n"
                    + ";");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("cycling"),
            eq(null),
            eq(Scope.CREATE),
            eq(expected),
            eq(ResourceKind.AGGREGATE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementCreateFunction(SourceAPI sourceApi)
      throws UnauthorizedException {
    CreateFunctionStatement.Raw rawStatement =
        (CreateFunctionStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE OR REPLACE FUNCTION cycling.fLog (\n"
                    + "  input double\n"
                    + ") \n"
                    + "  CALLED ON NULL INPUT \n"
                    + "  RETURNS double \n"
                    + "  LANGUAGE java AS \n"
                    + "    $$\n"
                    + "      return Double.valueOf(Math.log(input.doubleValue()));\n"
                    + "    $$ \n"
                    + ";");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("cycling"),
            eq(null),
            eq(Scope.CREATE),
            eq(expected),
            eq(ResourceKind.FUNCTION));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementCreateIndex(SourceAPI sourceApi)
      throws UnauthorizedException {
    CreateIndexStatement.Raw rawStatement =
        (CreateIndexStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE INDEX IF NOT EXISTS value_idx ON ks1.tbl1 (value);");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq("tbl1"),
            eq(Scope.CREATE),
            eq(expected),
            eq(ResourceKind.INDEX));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementCreateTrigger(SourceAPI sourceApi)
      throws UnauthorizedException {
    CreateTriggerStatement.Raw rawStatement =
        (CreateTriggerStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE TRIGGER trigger1\n"
                    + "  ON ks1.tbl1\n"
                    + "  USING 'org.apache.cassandra.triggers.AuditTrigger'");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq("tbl1"),
            eq(Scope.CREATE),
            eq(expected),
            eq(ResourceKind.TRIGGER));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementCreateType(SourceAPI sourceApi)
      throws UnauthorizedException {
    CreateTypeStatement.Raw rawStatement =
        (CreateTypeStatement.Raw)
            QueryProcessor.parseStatement("CREATE TYPE IF NOT EXISTS ks1.type1 (a text, b text);");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq(null),
            eq(Scope.CREATE),
            eq(expected),
            eq(ResourceKind.TYPE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementCreateView(SourceAPI sourceApi)
      throws UnauthorizedException {
    CreateViewStatement.Raw rawStatement =
        (CreateViewStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS cycling.cyclist_by_age AS\n"
                    + "  SELECT age, cid, birthday, country, name\n"
                    + "  FROM cycling.cyclist_base \n"
                    + "  WHERE age IS NOT NULL\n"
                    + "    AND cid IS NOT NULL\n"
                    + "  PRIMARY KEY (age, cid)\n"
                    + "  WITH CLUSTERING ORDER BY (cid ASC)\n"
                    + "  AND caching = {\n"
                    + "    'keys' : 'ALL',\n"
                    + "    'rows_per_partition' : '100'\n"
                    + "  }\n"
                    + "  AND comment = 'Based on table cyclist';");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("cycling"),
            eq(null),
            eq(Scope.CREATE),
            eq(expected),
            eq(ResourceKind.VIEW));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementDropAggregate(SourceAPI sourceApi)
      throws UnauthorizedException {
    DropAggregateStatement.Raw rawStatement =
        (DropAggregateStatement.Raw)
            QueryProcessor.parseStatement("DROP AGGREGATE IF EXISTS ks1.agg1;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq(null),
            eq(Scope.DROP),
            eq(expected),
            eq(ResourceKind.AGGREGATE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementDropFunction(SourceAPI sourceApi)
      throws UnauthorizedException {
    DropFunctionStatement.Raw rawStatement =
        (DropFunctionStatement.Raw)
            QueryProcessor.parseStatement("DROP FUNCTION IF EXISTS ks1.fLog;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq(null),
            eq(Scope.DROP),
            eq(expected),
            eq(ResourceKind.FUNCTION));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementDropIndex(SourceAPI sourceApi)
      throws UnauthorizedException {
    DropIndexStatement.Raw rawStatement =
        (DropIndexStatement.Raw)
            QueryProcessor.parseStatement("DROP INDEX IF EXISTS ks1.value_idx;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq(null),
            eq(Scope.DROP),
            eq(expected),
            eq(ResourceKind.INDEX));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementDropTrigger(SourceAPI sourceApi)
      throws UnauthorizedException {
    DropTriggerStatement.Raw rawStatement =
        (DropTriggerStatement.Raw)
            QueryProcessor.parseStatement("DROP TRIGGER IF EXISTS trigger1 ON ks1.tbl1;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq("tbl1"),
            eq(Scope.DROP),
            eq(expected),
            eq(ResourceKind.TRIGGER));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementDropType(SourceAPI sourceApi)
      throws UnauthorizedException {
    DropTypeStatement.Raw rawStatement =
        (DropTypeStatement.Raw) QueryProcessor.parseStatement("DROP TYPE IF EXISTS ks1.typ1;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq(null),
            eq(Scope.DROP),
            eq(expected),
            eq(ResourceKind.TYPE));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAlterSchemaStatementDropView(SourceAPI sourceApi)
      throws UnauthorizedException {
    DropViewStatement.Raw rawStatement =
        (DropViewStatement.Raw)
            QueryProcessor.parseStatement("DROP MATERIALIZED VIEW IF EXISTS ks1.view1;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationSubject),
            eq("ks1"),
            eq(null),
            eq(Scope.DROP),
            eq(expected),
            eq(ResourceKind.VIEW));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthorizationStatementPermissionsManagementStatement(SourceAPI sourceApi)
      throws UnauthorizedException {
    PermissionsManagementStatement.Raw rawStatement =
        QueryProcessor.parseStatement("GRANT ALL ON KEYSPACE ks1 TO role1;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizePermissionManagement(
            refEq(authenticationSubject),
            eq("data/ks1"),
            eq("roles/role1"),
            eq(Scope.AUTHORIZE),
            eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthorizationStatementListPermissionsStatement(SourceAPI sourceApi)
      throws UnauthorizedException {
    ListPermissionsStatement.Raw rawStatement =
        QueryProcessor.parseStatement("LIST ALL PERMISSIONS OF sam;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizePermissionRead(refEq(authenticationSubject), eq("roles/sam"), eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeListPermissionsException(SourceAPI sourceApi) throws UnauthorizedException {
    ListPermissionsStatement.Raw rawStatement =
        QueryProcessor.parseStatement("LIST ALL PERMISSIONS OF sam;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    Mockito.doThrow(new io.stargate.auth.UnauthorizedException("test-message"))
        .when(authorizationService)
        .authorizePermissionRead(any(), any(), any());

    assertThatThrownBy(() -> queryHandler.authorizeByToken(createToken(sourceApi), statement))
        .hasMessageContaining("test-message");
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthorizationStatementListRolesStatement(SourceAPI sourceApi)
      throws UnauthorizedException {
    ListRolesStatement.Raw rawStatement = QueryProcessor.parseStatement("LIST ROLES;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeRoleRead(refEq(authenticationSubject), eq(null), eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthorizationStatementRevokeRoleStatement(SourceAPI sourceApi)
      throws UnauthorizedException {
    RevokeRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement("REVOKE SELECT\n" + "ON KEYSPACE cycling \n" + "FROM coach;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizePermissionManagement(
            refEq(authenticationSubject),
            eq("data/cycling"),
            eq("roles/coach"),
            eq(Scope.AUTHORIZE),
            eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthorizationStatementGrantRoleStatement(SourceAPI sourceApi)
      throws UnauthorizedException {
    GrantRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement("GRANT ALTER\n" + "ON KEYSPACE cycling\n" + "TO coach;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizePermissionManagement(
            refEq(authenticationSubject),
            eq("data/cycling"),
            eq("roles/coach"),
            eq(Scope.AUTHORIZE),
            eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeAuthorizationStatementException(SourceAPI sourceApi) throws UnauthorizedException {
    GrantRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement("GRANT ALTER\n" + "ON KEYSPACE cycling\n" + "TO coach;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    Mockito.doThrow(new io.stargate.auth.UnauthorizedException("test-message"))
        .when(authorizationService)
        .authorizePermissionManagement(any(), any(), any(), any(), any());

    assertThatThrownBy(() -> queryHandler.authorizeByToken(createToken(sourceApi), statement))
        .hasMessageContaining("test-message");
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthorizationStatementListRolesStatementWithRole(SourceAPI sourceApi)
      throws UnauthorizedException {
    ListRolesStatement.Raw rawStatement = QueryProcessor.parseStatement("LIST ROLES OF coach;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeRoleRead(refEq(authenticationSubject), eq("roles/coach"), eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeRoleReadException(SourceAPI sourceApi) throws Exception {
    ListRolesStatement.Raw rawStatement = QueryProcessor.parseStatement("LIST ROLES OF coach;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    Mockito.doThrow(new io.stargate.auth.UnauthorizedException("test-message"))
        .when(authorizationService)
        .authorizeRoleRead(any(), any(), any());

    assertThatThrownBy(() -> queryHandler.authorizeByToken(createToken(sourceApi), statement))
        .hasMessageContaining("test-message");
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthenticationStatementRevokeRoleStatement(SourceAPI sourceApi)
      throws UnauthorizedException {
    RevokeRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement("REVOKE cycling_admin\n" + "FROM coach;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeRoleManagement(
            refEq(authenticationSubject),
            eq("roles/cycling_admin"),
            eq("roles/coach"),
            eq(Scope.AUTHORIZE),
            eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeRevokeRoleException(SourceAPI sourceApi) throws UnauthorizedException {
    RevokeRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement("REVOKE cycling_admin\n" + "FROM coach;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    Mockito.doThrow(new io.stargate.auth.UnauthorizedException("test-message"))
        .when(authorizationService)
        .authorizeRoleManagement(any(), any(), any(), any(), any());

    assertThatThrownBy(() -> queryHandler.authorizeByToken(createToken(sourceApi), statement))
        .hasMessageContaining("test-message");
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthenticationStatementGrantRoleStatement(SourceAPI sourceApi)
      throws UnauthorizedException {
    GrantRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement("GRANT cycling_admin\n" + "TO coach;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeRoleManagement(
            refEq(authenticationSubject),
            eq("roles/cycling_admin"),
            eq("roles/coach"),
            eq(Scope.AUTHORIZE),
            eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthenticationStatementGrantRoleStatementOnTable(SourceAPI sourceApi)
      throws UnauthorizedException {
    GrantRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement(
            "GRANT ALTER\n" + "ON TABLE cycling.cyclist_name\n" + "TO coach;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizePermissionManagement(
            refEq(authenticationSubject),
            eq("data/cycling/cyclist_name"),
            eq("roles/coach"),
            eq(Scope.AUTHORIZE),
            eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthenticationStatementDropRoleStatement(SourceAPI sourceApi)
      throws UnauthorizedException {
    DropRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement("DROP ROLE IF EXISTS team_manager;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeRoleManagement(
            refEq(authenticationSubject), eq("roles/team_manager"), eq(Scope.DROP), eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthenticationStatementCreateRoleStatement(SourceAPI sourceApi)
      throws UnauthorizedException {
    CreateRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement(
            "CREATE ROLE IF NOT EXISTS coach \n"
                + "WITH PASSWORD = 'All4One2day!' \n"
                + "  AND LOGIN = true;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeRoleManagement(
            refEq(authenticationSubject), eq("roles/coach"), eq(Scope.CREATE), eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenAuthenticationStatementAlterRoleStatement(SourceAPI sourceApi)
      throws UnauthorizedException {
    AlterRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement("ALTER ROLE sandy WITH PASSWORD = 'bestTeam';");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(1))
        .authorizeRoleManagement(
            refEq(authenticationSubject), eq("roles/sandy"), eq(Scope.ALTER), eq(expected));
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeAuthenticationStatementException(SourceAPI sourceApi) throws Exception {
    AlterRoleStatement.Raw rawStatement =
        QueryProcessor.parseStatement("ALTER ROLE sandy WITH PASSWORD = 'bestTeam';");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    Mockito.doThrow(new io.stargate.auth.UnauthorizedException("test-message"))
        .when(authorizationService)
        .authorizeRoleManagement(any(), any(), any(), any());

    assertThatThrownBy(() -> queryHandler.authorizeByToken(createToken(sourceApi), statement))
        .hasMessageContaining("test-message");
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenUseStatement(SourceAPI sourceApi) {
    UseStatement.Raw rawStatement = QueryProcessor.parseStatement("use ks1;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    verifyNoInteractions(authorizationService);
  }

  @ParameterizedTest
  @MethodSource("sourceApiValues")
  void authorizeByTokenBatchStatement(SourceAPI sourceApi) throws UnauthorizedException {
    BatchStatement.Raw rawStatement =
        QueryProcessor.parseStatement(
            "BEGIN BATCH\n"
                + "\n"
                + "  INSERT INTO ks1.tbl1 (\n"
                + "    key, value\n"
                + "  ) VALUES (\n"
                + "    'foo', 'bar'\n"
                + "  );\n"
                + "\n"
                + "  INSERT INTO ks1.tbl1 (\n"
                + "    key, value\n"
                + "  ) VALUES (\n"
                + "    'fizz', 'buzz'\n"
                + "  );\n"
                + "\n"
                + "  UPDATE ks1.tbl1\n"
                + "  SET value = 'baz'\n"
                + "  WHERE key = 'foo';\n"
                + "\n"
                + "APPLY BATCH;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(sourceApi), statement);

    SourceAPI expected = sourceApi != null ? sourceApi : SourceAPI.CQL;
    verify(authorizationService, times(3))
        .authorizeDataWrite(
            refEq(authenticationSubject), eq("ks1"), eq("tbl1"), eq(Scope.MODIFY), eq(expected));
  }

  public static Object[][] sourceApiValues() {
    return new Object[][] {
      new Object[] {null},
      new Object[] {SourceAPI.REST},
      new Object[] {SourceAPI.GRAPHQL},
      new Object[] {SourceAPI.CQL},
      new Object[] {SourceAPI.JSON}
    };
  }

  private Map<String, ByteBuffer> createToken() {
    return AuthenticatedUser.Serializer.serialize(authenticatedUser);
  }

  private Map<String, ByteBuffer> createToken(SourceAPI sourceAPI) {
    Map<String, ByteBuffer> payload = createToken();

    if (null == sourceAPI) {
      return payload;
    } else {
      HashMap<String, ByteBuffer> payloadWithSource = new HashMap<>(payload);
      sourceAPI.toCustomPayload(payloadWithSource);
      return payloadWithSource;
    }
  }
}
