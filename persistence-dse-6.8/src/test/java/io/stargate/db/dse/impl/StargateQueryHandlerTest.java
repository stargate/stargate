package io.stargate.db.dse.impl;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.stargate.auth.AuthenticationPrincipal;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.AuthenticatedUser;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
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
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StargateQueryHandlerTest extends BaseDseTest {

  AuthenticatedUser authenticatedUser = AuthenticatedUser.of("username", "token");
  AuthenticationPrincipal authenticationPrincipal =
      new AuthenticationPrincipal("token", "username");
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
    SchemaManager.instance.load(keyspaceMetadata);
  }

  @Test
  void authorizeByTokenSelectStatement() throws IOException, UnauthorizedException {
    SelectStatement.Raw rawStatement =
        (SelectStatement.Raw) QueryProcessor.parseStatement("select * from system.local");

    CQLStatement statement = rawStatement.prepare(false);

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeDataRead(refEq(authenticationPrincipal), eq("system_views"), eq("local_node"));
  }

  @Test
  void authorizeByTokenModificationStatementDelete() throws IOException, UnauthorizedException {
    DeleteStatement.Parsed rawStatement =
        (DeleteStatement.Parsed)
            QueryProcessor.parseStatement("delete from ks1.tbl1 where key = ?");

    CQLStatement statement =
        rawStatement.prepare(
            new VariableSpecifications(
                Collections.singletonList(new ColumnIdentifier("key", true))));

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeDataWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq("tbl1"), eq(Scope.DELETE));
  }

  @Test
  void authorizeByTokenModificationStatementModify() throws IOException, UnauthorizedException {
    UpdateStatement.Parsed rawStatement =
        (UpdateStatement.Parsed)
            QueryProcessor.parseStatement("update ks1.tbl1 set value = 'a' where key = ?");

    CQLStatement statement =
        rawStatement.prepare(
            new VariableSpecifications(
                Collections.singletonList(new ColumnIdentifier("key", true))));

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeDataWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq("tbl1"), eq(Scope.MODIFY));
  }

  @Test
  void authorizeByTokenTruncateStatement() throws IOException, UnauthorizedException {
    TruncateStatement.Raw rawStatement = QueryProcessor.parseStatement("truncate ks1.tbl1");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeDataWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq("tbl1"), eq(Scope.TRUNCATE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementCreateTable() throws IOException, UnauthorizedException {
    CreateTableStatement.Raw rawStatement =
        (CreateTableStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE TABLE IF NOT EXISTS ks1.tbl2 (key uuid PRIMARY KEY,value text);");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq("tbl2"), eq(Scope.CREATE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementDeleteTable() throws IOException, UnauthorizedException {
    DropTableStatement.Raw rawStatement =
        (Raw) QueryProcessor.parseStatement("drop table ks1.tbl1");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq("tbl1"), eq(Scope.DELETE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementAlterTable() throws IOException, UnauthorizedException {
    AlterTableStatement.Raw rawStatement =
        (AlterTableStatement.Raw)
            QueryProcessor.parseStatement("ALTER TABLE ks1.tbl1 ADD val2 INT");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq("tbl1"), eq(Scope.ALTER));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementCreateKeyspace()
      throws IOException, UnauthorizedException {
    CreateKeyspaceStatement.Raw rawStatement =
        (CreateKeyspaceStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE KEYSPACE ks2 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks2"), eq(null), eq(Scope.CREATE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementDeleteKeyspace()
      throws IOException, UnauthorizedException {
    DropKeyspaceStatement.Raw rawStatement =
        (DropKeyspaceStatement.Raw) QueryProcessor.parseStatement("drop keyspace ks1");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq(null), eq(Scope.DELETE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementAlterKeyspace()
      throws IOException, UnauthorizedException {
    AlterKeyspaceStatement.Raw rawStatement =
        (AlterKeyspaceStatement.Raw)
            QueryProcessor.parseStatement(
                "ALTER KEYSPACE ks1 WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'SearchAnalytics' : 1 } AND DURABLE_WRITES = false;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(refEq(authenticationPrincipal), eq("ks1"), eq(null), eq(Scope.ALTER));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementAlterType() throws IOException, UnauthorizedException {
    AlterTypeStatement.Raw rawStatement =
        (AlterTypeStatement.Raw)
            QueryProcessor.parseStatement("ALTER TYPE cycling.fullname ADD middlename text;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("cycling"), eq(null), eq(Scope.ALTER));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementAlterView() throws IOException, UnauthorizedException {
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

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("cycling"), eq("cyclist_by_age"), eq(Scope.ALTER));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementCreateAggregate()
      throws IOException, UnauthorizedException {
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

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("cycling"), eq(null), eq(Scope.CREATE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementCreateFunction()
      throws IOException, UnauthorizedException {
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

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("cycling"), eq(null), eq(Scope.CREATE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementCreateIndex() throws IOException, UnauthorizedException {
    CreateIndexStatement.Raw rawStatement =
        (CreateIndexStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE INDEX IF NOT EXISTS value_idx ON ks1.tbl1 (value);");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq("tbl1"), eq(Scope.CREATE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementCreateTrigger()
      throws IOException, UnauthorizedException {
    CreateTriggerStatement.Raw rawStatement =
        (CreateTriggerStatement.Raw)
            QueryProcessor.parseStatement(
                "CREATE TRIGGER trigger1\n"
                    + "  ON ks1.tbl1\n"
                    + "  USING 'org.apache.cassandra.triggers.AuditTrigger'");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq("tbl1"), eq(Scope.CREATE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementCreateType() throws IOException, UnauthorizedException {
    CreateTypeStatement.Raw rawStatement =
        (CreateTypeStatement.Raw)
            QueryProcessor.parseStatement("CREATE TYPE IF NOT EXISTS ks1.type1 (a text, b text);");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq(null), eq(Scope.CREATE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementCreateView() throws IOException, UnauthorizedException {
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

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("cycling"), eq("cyclist_by_age"), eq(Scope.CREATE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementDropAggregate()
      throws IOException, UnauthorizedException {
    DropAggregateStatement.Raw rawStatement =
        (DropAggregateStatement.Raw)
            QueryProcessor.parseStatement("DROP AGGREGATE IF EXISTS ks1.agg1;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq(null), eq(Scope.DELETE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementDropFunction()
      throws IOException, UnauthorizedException {
    DropFunctionStatement.Raw rawStatement =
        (DropFunctionStatement.Raw)
            QueryProcessor.parseStatement("DROP FUNCTION IF EXISTS ks1.fLog;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq(null), eq(Scope.DELETE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementDropIndex() throws IOException, UnauthorizedException {
    DropIndexStatement.Raw rawStatement =
        (DropIndexStatement.Raw)
            QueryProcessor.parseStatement("DROP INDEX IF EXISTS ks1.value_idx;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq(null), eq(Scope.DELETE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementDropTrigger() throws IOException, UnauthorizedException {
    DropTriggerStatement.Raw rawStatement =
        (DropTriggerStatement.Raw)
            QueryProcessor.parseStatement("DROP TRIGGER IF EXISTS trigger1 ON ks1.tbl1;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq("tbl1"), eq(Scope.DELETE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementDropType() throws IOException, UnauthorizedException {
    DropTypeStatement.Raw rawStatement =
        (DropTypeStatement.Raw) QueryProcessor.parseStatement("DROP TYPE IF EXISTS ks1.typ1;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq(null), eq(Scope.DELETE));
  }

  @Test
  void authorizeByTokenAlterSchemaStatementDropView() throws IOException, UnauthorizedException {
    DropViewStatement.Raw rawStatement =
        (DropViewStatement.Raw)
            QueryProcessor.parseStatement("DROP MATERIALIZED VIEW IF EXISTS ks1.view1;");

    CQLStatement statement = rawStatement.prepare(ClientState.forInternalCalls());

    queryHandler.authorizeByToken(createToken(), statement);
    verify(authorizationService, times(1))
        .authorizeSchemaWrite(
            refEq(authenticationPrincipal), eq("ks1"), eq("view1"), eq(Scope.DELETE));
  }

  private ByteBuffer createToken() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
    objectOutputStream.writeObject(authenticatedUser);
    objectOutputStream.flush();
    byte[] bytes = outputStream.toByteArray();

    InputStream inputStream = new ByteArrayInputStream(bytes);
    ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
    while (inputStream.available() > 0) {
      byteBuffer.put((byte) inputStream.read());
    }

    byteBuffer.flip();
    return byteBuffer;
  }
}
