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
package io.stargate.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

@NotThreadSafe
public class CQLTest extends BaseOsgiIntegrationTest {
  private String table;
  private String keyspace;
  private CqlSession session;

  /**
   * Re-enable authentication for {@Link CQLTest#invalidCredentials()} and {@Link
   * CQLTest#tokenAuthentication()} whenever authentcation is supported by our backend C* container
   * and/or we decide to use ccm.
   */
  public CQLTest(ClusterConnectionInfo backend) {
    // enableAuth = true;
    super(backend);
  }

  @BeforeAll
  public static void beforeAll() {
    System.setProperty("stargate.cql_use_auth_service", "true");
  }

  @BeforeEach
  public void setup(TestInfo testInfo) {
    Optional<String> name = testInfo.getTestMethod().map(Method::getName);
    assertThat(name).isPresent();
    String testName = name.get();
    keyspace = "ks_" + new Date().getTime() + "_" + testName;

    if (keyspace.length() > KEYSPACE_NAME_MAX_LENGTH) {
      keyspace = keyspace.substring(0, KEYSPACE_NAME_MAX_LENGTH);
    }

    table = testName;
  }

  @BeforeEach
  public void before() {
    session =
        CqlSession.builder()
            .withConfigLoader(
                getDriverConfigLoaderBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(1))
                    .build())
            .withAuthCredentials("cassandra", "cassandra")
            .addContactPoint(new InetSocketAddress(getStargateHost(), 9043))
            .build();
  }

  @AfterEach
  public void after() {
    if (session != null) {
      session.close();
    }
  }

  private void createKeyspace() {
    session.execute(
        String.format(
            "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
            keyspace));
  }

  private void createTable() {
    createKeyspace();
    session.execute(
        String.format(
            "CREATE TABLE \"%s\".\"%s\" (key text PRIMARY KEY, value1 text, value2 text)",
            keyspace, table));
  }

  private void paginationTestSetup() {
    createKeyspace();

    session.execute(
        SimpleStatement.newInstance(
            String.format(
                "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (k int, cc int, v int, PRIMARY KEY(k, cc))",
                keyspace, table)));
    for (int i = 0; i < 20; i++) {
      session.execute(
          SimpleStatement.newInstance(
              String.format(
                  "INSERT INTO \"%s\".\"%s\" (k, cc, v) VALUES (1, ?, ?)", keyspace, table),
              i,
              i));
    }
  }

  private String insertIntoQuery() {
    return String.format(
        "INSERT INTO \"%s\".\"%s\" (key, value1, value2) values (:key, :value1, :value2)",
        keyspace, table);
  }

  private String insertIntoQueryNoKeyspace() {
    return String.format(
        "INSERT INTO \"%s\" (key, value1, value2) values (:key, :value1, :value2)", table);
  }

  private String selectFromQuery(boolean withKey) {
    return String.format(
        "SELECT * FROM \"%s\".\"%s\"%s", keyspace, table, withKey ? " WHERE key = :key" : "");
  }

  private String selectFromQueryNoKeyspace() {
    return String.format("SELECT * FROM \"%s\"", table);
  }

  @Test
  public void querySystemLocal() throws UnknownHostException {
    ResultSet rs = session.execute("SELECT * FROM system.local");
    Iterator<Row> rows = rs.iterator();
    assertThat(rows).hasNext();
    assertThat(rows.next().getInetAddress("listen_address")).isIn(getStargateInetSocketAddresses());
  }

  public enum QueryMode {
    SIMPLE_POSITIONAL,
    SIMPLE_BY_NAME,
    PREPARED_POSITIONAL,
    PREPARED_BY_NAME;

    boolean isPrepared() {
      return this == PREPARED_POSITIONAL || this == PREPARED_BY_NAME;
    }

    boolean isBindByName() {
      return this == SIMPLE_BY_NAME || this == PREPARED_BY_NAME;
    }
  }

  private Statement<?> insertIntoStatement(QueryMode mode, String... args) {
    assert args.length > 0 : "Expecting at least the key";
    assert args.length <= 3 : "Expecting at most the key, value1 and value2";

    if (mode.isPrepared()) {
      PreparedStatement insertPrepared = session.prepare(insertIntoQuery());
      BoundStatement statement;
      if (mode.isBindByName()) {
        statement = insertPrepared.bind().setString("key", args[0]);
        for (int i = 1; i < args.length; i++) {
          statement = statement.setString("value" + i, args[i]);
        }
      } else {
        statement = insertPrepared.bind((Object[]) args);
      }
      return statement;
    } else {
      SimpleStatementBuilder insertBuilder = SimpleStatement.builder(insertIntoQuery());
      if (mode.isBindByName()) {
        insertBuilder.addNamedValue("key", args[0]);
        for (int i = 1; i < args.length; i++) {
          insertBuilder.addNamedValue("value" + i, args[i]);
        }
      } else {
        insertBuilder.addPositionalValues((Object[]) args);
      }
      return insertBuilder.build();
    }
  }

  private Statement<?> selectByKeyStatement(QueryMode mode, String key) {
    if (mode.isPrepared()) {
      PreparedStatement selectPrepared = session.prepare(selectFromQuery(true));
      BoundStatement statement;
      if (mode.isBindByName()) {
        statement = selectPrepared.bind().setString("key", key);
      } else {
        statement = selectPrepared.bind(key);
      }
      return statement;
    } else {
      SimpleStatementBuilder selectBuilder = SimpleStatement.builder(selectFromQuery(true));
      if (mode.isBindByName()) {
        selectBuilder.addNamedValue("key", key);
      } else {
        selectBuilder.addPositionalValue(key);
      }
      return selectBuilder.build();
    }
  }

  @ParameterizedTest
  @EnumSource(QueryMode.class)
  public void query(QueryMode mode) {
    createTable();

    session.execute(insertIntoStatement(mode, "abc", "v1", "v2"));
    ResultSet rs = session.execute(selectByKeyStatement(mode, "abc"));

    assertResultSet(rs)
        .row()
        .value("key", "abc")
        .value("value1", "v1")
        .value("value2", "v2")
        .done();
  }

  // Note: in theory, unset could work with non-prepared statements, but it appears the driver
  // don't support them (it doesn't set an 'unset' value for missing values like it does with
  // prepared statements, and so we end up with a "not enough values" error).
  @ParameterizedTest
  @EnumSource(
      value = QueryMode.class,
      names = {"PREPARED_POSITIONAL", "PREPARED_BY_NAME"})
  public void unsetValues(QueryMode mode) {
    createTable();

    session.execute(insertIntoStatement(mode, "k1", "v11", "v21"));

    // Sanity check
    assertResultSet(session.execute(selectByKeyStatement(mode, "k1")))
        .row()
        .value("key", "k1")
        .value("value1", "v11")
        .value("value2", "v21")
        .done();

    session.execute(insertIntoStatement(mode, "k1", "v12"));

    assertResultSet(session.execute(selectByKeyStatement(mode, "k1")))
        .row()
        .value("key", "k1")
        .value("value1", "v12")
        .value("value2", "v21")
        .done();
  }

  @Test
  public void batchMixed() {
    createTable();

    PreparedStatement insertPrepared = session.prepare(insertIntoQuery());

    BatchStatement batch =
        BatchStatement.builder(BatchType.UNLOGGED)
            .addStatement(insertPrepared.bind("abc", "def", "ghi"))
            .addStatement(
                SimpleStatement.builder(insertIntoQuery())
                    .addPositionalValues("def", "ghi", "jkl")
                    .build())
            .build();

    session.execute(batch);

    ResultSet rs = session.execute(selectFromQuery(false));

    List<Row> rows = rs.all();
    assertThat(rows).hasSize(2);

    assertThat(rows.get(0).getString("key")).isEqualTo("def");
    assertThat(rows.get(0).getString("value1")).isEqualTo("ghi");
    assertThat(rows.get(0).getString("value2")).isEqualTo("jkl");

    assertThat(rows.get(1).getString("key")).isEqualTo("abc");
    assertThat(rows.get(1).getString("value1")).isEqualTo("def");
    assertThat(rows.get(1).getString("value2")).isEqualTo("ghi");
  }

  @Test
  public void schemaMetadataEvents() throws InterruptedException {
    assertThat(session.getMetadata().getKeyspace(keyspace)).isNotPresent();

    createTable();

    KeyspaceMetadata ksMetadata =
        waitFor(() -> session.getMetadata().getKeyspace(CqlIdentifier.fromInternal(keyspace)));
    TableMetadata tableMetadata =
        waitFor(() -> ksMetadata.getTable(CqlIdentifier.fromInternal(table)));

    assertThat(tableMetadata.getColumns()).hasSize(3);
  }

  @Test
  public void serverSideWarnings() {
    createKeyspace();

    // Create a table with an integer value for the `sum()` aggregate
    session.execute(
        String.format(
            "CREATE TABLE \"%s\".\"%s\" (key text PRIMARY KEY, value1 int, value2 text)",
            keyspace, table));

    session.execute(
        SimpleStatement.builder(insertIntoQuery())
            .setTracing(true)
            .addPositionalValues("abc", 42, "foo")
            .build());

    ResultSet rs =
        session.execute(String.format("SELECT sum(value1) FROM \"%s\".\"%s\"", keyspace, table));
    List<String> warnings = rs.getExecutionInfo().getWarnings();
    assertThat(warnings).hasSize(1);
    assertThat(warnings).contains("Aggregation query used without partition key");
  }

  @Test
  public void traceQuery() throws UnknownHostException {
    createTable();

    ResultSet rs =
        session.execute(
            SimpleStatement.builder(insertIntoQuery())
                .setTracing(true)
                .addPositionalValues("abc", "def", "ghi")
                .build());

    assertThat(rs.getExecutionInfo().getTracingId()).isNotNull();

    QueryTrace trace = rs.getExecutionInfo().getQueryTrace();
    assertThat(trace.getCoordinatorAddress().getAddress()).isIn(getStargateInetSocketAddresses());
    assertThat(trace.getRequestType()).isEqualTo("Execute CQL3 query");
    assertThat(trace.getEvents()).isNotEmpty();
  }

  @Test
  public void tracePrepare() throws UnknownHostException {
    createTable();

    PreparedStatement insertPrepared = session.prepare(insertIntoQuery());
    ResultSet rs = session.execute(insertPrepared.bind("abc", "def").setTracing(true));

    assertThat(rs.getExecutionInfo().getTracingId()).isNotNull();

    QueryTrace trace = rs.getExecutionInfo().getQueryTrace();
    assertThat(trace.getCoordinatorAddress().getAddress()).isIn(getStargateInetSocketAddresses());
    assertThat(trace.getRequestType()).isEqualTo("Execute CQL3 prepared query");
    assertThat(trace.getEvents()).isNotEmpty();
  }

  @Test
  public void traceBatch() throws UnknownHostException {
    createTable();

    PreparedStatement insertPrepared = session.prepare(insertIntoQuery());

    BatchStatement batch =
        BatchStatement.builder(BatchType.UNLOGGED)
            .addStatement(insertPrepared.bind("abc", "def", "ghi"))
            .addStatement(
                SimpleStatement.builder(insertIntoQuery())
                    .addPositionalValues("def", "ghi", "jkl")
                    .build())
            .setTracing(true)
            .build();

    ResultSet rs = session.execute(batch);
    QueryTrace trace = rs.getExecutionInfo().getQueryTrace();
    assertThat(trace.getCoordinatorAddress().getAddress()).isIn(getStargateInetSocketAddresses());
    assertThat(trace.getRequestType()).isEqualTo("Execute batch of CQL3 queries");
    assertThat(trace.getEvents()).isNotEmpty();
  }

  @Test
  public void useKeyspace() {
    createTable();

    try {
      session.execute(
          SimpleStatement.builder(insertIntoQueryNoKeyspace())
              .addPositionalValues("abc", "def", "ghi")
              .build());

      fail("Should have thrown InvalidQueryException");
    } catch (InvalidQueryException ex) {
      assertThat(ex)
          .hasMessage(
              "No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
    }

    // Switch to keyspace and retry query

    session.execute(String.format("USE \"%s\"", keyspace));

    session.execute(
        SimpleStatement.builder(insertIntoQueryNoKeyspace())
            .addPositionalValues("abc", "def", "ghi")
            .build());

    ResultSet rs = session.execute(selectFromQueryNoKeyspace());

    Iterator<Row> rows = rs.iterator();
    assertThat(rows).hasNext();

    Row row = rows.next();
    assertThat(row.getString("key")).isEqualTo("abc");
    assertThat(row.getString("value1")).isEqualTo("def");
    assertThat(row.getString("value2")).isEqualTo("ghi");
  }

  @Test
  public void tupleTest() {
    createKeyspace();
    session.execute(
        String.format(
            "CREATE TABLE \"%s\".\"%s\" (key text PRIMARY KEY, value1 tuple<text,int,int>, value2 text)",
            keyspace, table));

    TupleType tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.INT, DataTypes.INT);
    TupleValue tupleValue = tupleType.newValue("hello", 1, 2);
    session.execute(
        SimpleStatement.builder(insertIntoQuery())
            .setTracing(true)
            .addPositionalValues("abc", tupleValue, "foo")
            .build());

    ResultSet rs =
        session.execute(
            SimpleStatement.builder(selectFromQuery(true)).addPositionalValue("abc").build());

    Iterator<Row> rows = rs.iterator();
    assertThat(rows).hasNext();

    Row row = rows.next();
    assertThat(row.getString("key")).isEqualTo("abc");

    TupleValue tupleReturnValue = row.getTupleValue("value1");
    assertThat(tupleReturnValue).isNotNull();
    assertThat(tupleReturnValue.getString(0)).isEqualTo("hello");
    assertThat(tupleReturnValue.getInt(1)).isEqualTo(1);
    assertThat(tupleReturnValue.getInt(2)).isEqualTo(2);

    assertThat(row.getString("value2")).isEqualTo("foo");
  }

  @ParameterizedTest
  @ValueSource(strings = {"lz4", "snappy"})
  public void compressionTest(String compression) {
    try (CqlSession session =
        CqlSession.builder()
            .withConfigLoader(
                getDriverConfigLoaderBuilder()
                    .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compression)
                    .build())
            .addContactPoint(new InetSocketAddress(getStargateHost(), 9043))
            .build()) {
      ResultSet rs = session.execute("SELECT * FROM system.local");
      assertThat(rs.one().getString("key")).isEqualTo("local");
    }
  }

  @Test
  public void udtTest() {
    createKeyspace();
    session.execute(
        String.format("CREATE TYPE \"%s\".address (street text, city text, zip int)", keyspace));
    String tableName = String.format("\"%s\".\"%s\"", keyspace, table);
    session.execute(
        String.format("CREATE TABLE %s (key int PRIMARY KEY, value address)", tableName));

    UserDefinedType udt =
        session
            .getMetadata()
            .getKeyspace(CqlIdentifier.fromInternal(keyspace))
            .flatMap(ks -> ks.getUserDefinedType("address"))
            .orElseThrow(() -> new IllegalArgumentException("Missing UDT definition"));

    UdtValue udtValue = udt.newValue("Street 1", "City 1", 90678);
    session.execute(
        SimpleStatement.newInstance(
            String.format("INSERT INTO %s (key, value) VALUES (?, ?)", tableName), 1, udtValue));

    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                String.format("SELECT * FROM %s WHERE key = ?", tableName), 1));
    assertThat(rs.one().getUdtValue("value")).isEqualTo(udtValue);
  }

  @Test
  public void shouldUsePaginationForBoundStatement() {
    paginationTestSetup();

    BoundStatement boundStatement =
        session
            .prepare(
                SimpleStatement.newInstance(
                        String.format("SELECT * FROM \"%s\".\"%s\" WHERE k = ?", keyspace, table))
                    .setPageSize(15))
            .bind(1);

    // fetch the first page
    ResultSet resultSet = session.execute(boundStatement);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(15);
    assertThat(resultSet.isFullyFetched()).isFalse();

    ByteBuffer pagingState = resultSet.getExecutionInfo().getPagingState();

    // fetch the last page
    resultSet = session.execute(boundStatement.setPagingState(pagingState));
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(5);
    assertThat(resultSet.isFullyFetched()).isTrue();
  }

  @Test
  public void shouldUsePaginationForSimpleStatement() {
    paginationTestSetup();

    String selectQuery = String.format("SELECT * FROM \"%s\".\"%s\" WHERE k = ?", keyspace, table);
    PreparedStatement selectPs = session.prepare(selectQuery);

    Statement<?> stmt = SimpleStatement.newInstance(selectQuery, 1);
    // fetch the first page
    ResultSet rs = session.execute(stmt.setPageSize(15));
    assertThat(rs.getAvailableWithoutFetching()).isEqualTo(15);
    ByteBuffer pageState = rs.getExecutionInfo().getPagingState();
    assertThat(pageState).isNotNull();

    // fetch the last page
    rs = session.execute(selectPs.bind(1).setPageSize(15).setPagingState(pageState));
    assertThat(rs.getAvailableWithoutFetching()).isEqualTo(5);
    assertThat(rs.getExecutionInfo().getPagingState()).isNull();
  }

  @Test
  public void tooFewBindVariables() {
    createTable();

    assertThatThrownBy(
            () -> {
              session.execute(session.prepare(selectFromQuery(true)).bind()); // No variable when one is required
            })
        .isInstanceOf(InvalidQueryException.class)
        .hasMessage("Invalid unset value for column key");

    assertThatThrownBy(
            () -> {
              session.execute(
                  new SimpleStatementBuilder(
                          selectFromQuery(true)) // No variable when one is required
                      .build());
            })
        .isInstanceOf(InvalidQueryException.class)
        .hasMessage("there were 1 markers(?) in CQL but 0 bound variables");
  }

  @Test
  public void tooManyBindVariables() {
    createTable();

    assertThatThrownBy(
        () -> {
          session.execute(
              new SimpleStatementBuilder(
                  selectFromQuery(true))
                  .addPositionalValue("abc")
                  .addPositionalValue("def") // Too many variables
                  .build());
        })
        .isInstanceOf(InvalidQueryException.class)
        .hasMessage("there were 1 markers(?) in CQL but 2 bound variables");
  }

  @Disabled("Enable when persistence backends support auth in tests")
  @Test
  public void invalidCredentials() {
    try {
      try (CqlSession session =
          CqlSession.builder()
              .withConfigLoader(getDriverConfigLoaderBuilder().build())
              .withAuthCredentials("invalid", "invalid")
              .addContactPoint(new InetSocketAddress(getStargateHost(), 9043))
              .build()) {
        fail("Should have failed with AllNodesFailedException");
      }
    } catch (AllNodesFailedException e) {
      assertThat(e).hasMessageContaining("Provided username invalid and/or password are incorrect");
    } catch (Exception e) {
      fail("Failed with invalid exception type", e);
    }
  }

  @Disabled("Enable when persistence backends support auth in tests")
  @Test
  public void tokenAuthentication() throws IOException {
    String authToken = getAuthToken();
    assertThat(authToken).isNotEmpty();
    try (CqlSession session =
        CqlSession.builder()
            .withConfigLoader(getDriverConfigLoaderBuilder().build())
            .withAuthCredentials("token", authToken)
            .addContactPoint(new InetSocketAddress(getStargateHost(), 9043))
            .build()) {
      ResultSet rs = session.execute("SELECT * FROM system.local");
      Iterator<Row> rows = rs.iterator();
      assertThat(rows).hasNext();
      assertThat(rows.next().getInetAddress("listen_address"))
          .isEqualTo(InetAddress.getByName(getStargateHost()));
    }
  }

  private static <T> T waitFor(Supplier<Optional<T>> supplier) throws InterruptedException {
    for (int i = 0; i < 100; ++i) {
      Optional<T> v = supplier.get();
      if (v.isPresent()) {
        return v.get();
      }
      Thread.sleep(100);
    }

    fail("Time elapsed waiting for test condition");
    return null;
  }

  private static ProgrammaticDriverConfigLoaderBuilder getDriverConfigLoaderBuilder() {
    return DriverConfigLoader.programmaticBuilder()
        .withBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, false)
        .withString(
            DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
            DcInferringLoadBalancingPolicy.class.getName())
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
        .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(5));
  }

  private String getAuthToken() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", "http://" + getStargateHost()),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    return authTokenResponse.getAuthToken();
  }

  private static ResultSetAsserter assertResultSet(ResultSet rs) {
    return new ResultSetAsserter(rs);
  }

  static class ResultSetAsserter {
    private final Iterator<Row> actualRows;
    private int returnedRows;

    ResultSetAsserter(ResultSet actual) {
      this.actualRows = actual.iterator();
    }

    void isEmpty() {
      assert returnedRows == 0 : "Don't use isEmpty() and row() on the same asserter";
      assertThat(actualRows).isExhausted();
    }

    RowAsserter row() {
      return new RowAsserter();
    }

    class RowAsserter {
      private final Row actualRow;

      private RowAsserter() {
        ResultSetAsserter that = ResultSetAsserter.this;
        assertThat(that.actualRows).hasNext();
        that.returnedRows++;
        this.actualRow = that.actualRows.next();
      }

      private RowAsserter value(String name, Object expectedValue) {
        assertThat(actualRow.getObject(name)).isEqualTo(expectedValue);
        return this;
      }

      private RowAsserter andRow() {
        return new RowAsserter();
      }

      private void done() {
        assertThat(ResultSetAsserter.this.actualRows).isExhausted();
      }
    }
  }
}
