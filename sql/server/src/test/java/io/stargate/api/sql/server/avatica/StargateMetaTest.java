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
package io.stargate.api.sql.server.avatica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.eq;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.stargate.api.sql.AbstractDataStoreTest;
import io.stargate.api.sql.schema.TypeUtils;
import io.stargate.api.sql.server.avatica.SerializingTestDriver.SerializationParams;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Column;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class StargateMetaTest extends AbstractDataStoreTest {

  private static final String TEST_USER = "test_validated_user";
  private static final String TEST_PASSWORD = "test_token";
  private static final String JDBC_TOKEN_USER = "token";

  private final AuthenticationService authenticator;
  private StargateMeta stargateMeta;

  public StargateMetaTest() throws Exception {
    authenticator = Mockito.mock(AuthenticationService.class);
    Mockito.when(authenticator.validateToken(Mockito.eq(TEST_PASSWORD)))
        .thenReturn(new StoredCredentials().roleName(TEST_USER));
  }

  @BeforeEach
  public void createMeta() {
    stargateMeta = new StargateMeta(this::dataStore, authenticator);
  }

  public static Stream<Arguments> allColumnParams() {
    return Arrays.stream(SerializationParams.values())
        .flatMap(
            s ->
                sampleValues(table3, true).entrySet().stream()
                    .filter(e -> e.getKey().startsWith("c_"))
                    .map(e -> Arguments.of(s, e.getKey(), e.getValue())));
  }

  private Connection newConnection(SerializationParams serialization, AtomicLong roundTripCounter)
      throws SQLException {
    return newConnection(serialization, JDBC_TOKEN_USER, TEST_PASSWORD, roundTripCounter);
  }

  private Connection newConnection(SerializationParams serialization) throws SQLException {
    return newConnection(serialization, JDBC_TOKEN_USER, TEST_PASSWORD);
  }

  private Connection newConnection(SerializationParams ser, String user, String password)
      throws SQLException {
    return newConnection(ser, user, password, new AtomicLong());
  }

  private Connection newConnection(
      SerializationParams ser, String user, String password, AtomicLong roundTripCounter)
      throws SQLException {
    return SerializingTestDriver.newConnection(ser, stargateMeta, user, password, roundTripCounter);
  }

  private DataStore dataStore(String username) {
    assertThat(username).isEqualTo(TEST_USER);
    return dataStore;
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void openConnection(SerializationParams ser) throws SQLException {
    assertThat(newConnection(ser)).isNotNull();
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void openConnectionWithWrongCredentials(SerializationParams ser) throws Exception {
    Mockito.when(authenticator.validateToken(eq("bad_token")))
        .thenThrow(new UnauthorizedException("test-message"));
    assertThatThrownBy(() -> newConnection(ser, JDBC_TOKEN_USER, "bad_token"))
        .hasMessageContaining("test-message");
    assertThatThrownBy(() -> newConnection(ser, "some_user", "bad_token"))
        .hasMessageContaining("Unexpected user name for token authentication: some_user");
    assertThatThrownBy(() -> newConnection(ser, "test_user", null))
        .hasMessageContaining("Missing credentials in connection properties");
    assertThatThrownBy(() -> newConnection(ser, null, TEST_PASSWORD))
        .hasMessageContaining("Missing credentials in connection properties");
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void simpleExecute(SerializationParams serialization) throws SQLException {
    withTwoRowsInTable1();
    Connection connection = newConnection(serialization);
    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery("select * from test_ks.test1");
    assertTest1Data(rs);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void prepareAndExecute(SerializationParams serialization) throws SQLException {
    withTwoRowsInTable1();
    Connection connection = newConnection(serialization);
    PreparedStatement statement = connection.prepareStatement("select * from test_ks.test1");
    ResultSet rs = statement.executeQuery();
    assertTest1Data(rs);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void prepareAndExecuteWithBindVariables(SerializationParams serialization)
      throws SQLException {
    withTwoRowsInTable1();

    Connection connection = newConnection(serialization);
    PreparedStatement statement =
        connection.prepareStatement("select * from test_ks.test1 where a > ? AND a < ?");

    statement.setInt(1, 19);
    statement.setInt(2, 21);
    ResultSet rs = statement.executeQuery();

    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt(1)).isEqualTo(20);

    assertThat(rs.next()).isFalse();
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void longResultSet(SerializationParams serialization) throws SQLException {
    List<Map<String, Object>> rows =
        IntStream.range(0, 1000)
            .mapToObj(i -> ImmutableMap.<String, Object>of("a", i))
            .collect(Collectors.toList());
    withQuery(table1, "SELECT a FROM %s").returning(rows);

    AtomicLong roundTripCounter = new AtomicLong();
    Connection connection = newConnection(serialization, roundTripCounter);
    PreparedStatement statement =
        connection.prepareStatement("select a from test_ks.test1 order by a ASC");
    ResultSet rs = statement.executeQuery();

    long count0 = roundTripCounter.get();
    for (int i = 0; i < 1000; i++) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt(1)).isEqualTo(i);
    }

    // expect several round trips to the server during interation since the default Frame size
    // in Avatica is 100 rows.
    assertThat(roundTripCounter.get()).isGreaterThan(count0);

    assertThat(rs.next()).isFalse();
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void nullableColumnFlags(SerializationParams serialization) throws SQLException {
    withQuery(table1, "SELECT pk, cc1, cc2, val FROM test_ks.test4").returningNothing();

    Connection connection = newConnection(serialization);
    PreparedStatement statement =
        connection.prepareStatement("select pk, cc1, cc2, val from test_ks.test4");

    ResultSetMetaData metaData = statement.getMetaData();

    assertThat(metaData.getColumnName(1)).isEqualTo("pk");
    assertThat(metaData.isNullable(1)).isEqualTo(ResultSetMetaData.columnNoNulls);

    assertThat(metaData.getColumnName(2)).isEqualTo("cc1");
    assertThat(metaData.isNullable(2)).isEqualTo(ResultSetMetaData.columnNoNulls);
    assertThat(metaData.getColumnName(3)).isEqualTo("cc2");
    assertThat(metaData.isNullable(3)).isEqualTo(ResultSetMetaData.columnNoNulls);

    assertThat(metaData.getColumnName(4)).isEqualTo("val");
    assertThat(metaData.isNullable(4)).isEqualTo(ResultSetMetaData.columnNullable);

    ignorePreparedExecutions();
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void explainPlan(SerializationParams ser) throws SQLException {
    withAnySelectFrom(table1).returningNothing();
    withAnySelectFrom(table2).returningNothing();

    Connection connection = newConnection(ser);
    Statement statement = connection.createStatement();

    ResultSet rs = statement.executeQuery("EXPLAIN PLAN FOR select * from test_ks.test1");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getString(1)).contains("FullScan");
    assertThat(rs.getString(1)).contains("test1");
    assertThat(rs.next()).isFalse();

    rs =
        statement.executeQuery(
            "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR select * from test_ks.test1");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getString(1)).contains("LogicalTableScan");
    assertThat(rs.getString(1)).contains("test1");
    assertThat(rs.next()).isFalse();

    rs = statement.executeQuery("EXPLAIN PLAN WITH TYPE FOR select * from test_ks.test2");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getString(1)).contains("x INTEGER");
    assertThat(rs.getString(1)).contains("y VARCHAR");
    assertThat(rs.next()).isFalse();

    ignorePreparedExecutions();
  }

  private void withTwoRowsInTable1() {
    withQuery(table1, "SELECT a FROM %s")
        .returning(ImmutableList.of(ImmutableMap.of("a", 20), ImmutableMap.of("a", 10)));
  }

  private void assertTest1Data(ResultSet rs) throws SQLException {
    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt(1)).isEqualTo(20);

    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt(1)).isEqualTo(10);

    assertThat(rs.next()).isFalse();
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void simpleInsert(SerializationParams serialization) throws SQLException {
    Connection connection = newConnection(serialization);
    Statement statement = connection.createStatement();

    withAnyInsertInfo(table2).returningNothing();

    int updateCount = statement.executeUpdate("INSERT INTO test_ks.test2 (x, y) VALUES (1, 'a')");
    assertThat(updateCount).isEqualTo(1);

    boolean hasResultSet = statement.execute("INSERT INTO test_ks.test2 (x, y) VALUES (1, 'a')");
    assertThat(hasResultSet).isFalse();
    assertThat(statement.getUpdateCount()).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void closeStatement(SerializationParams serialization) throws SQLException {
    Connection connection = newConnection(serialization);
    Statement statement = connection.createStatement();

    assertThat(stargateMeta.connections()).hasSize(1);
    assertThat(stargateMeta.connections().values().iterator().next().statements()).hasSize(1);

    statement.close();

    assertThat(stargateMeta.connections()).hasSize(1);
    assertThat(stargateMeta.connections().values().iterator().next().statements()).hasSize(0);

    assertThatThrownBy(() -> statement.executeQuery("select * from test_ks.test1"))
        .hasMessage("Statement closed");
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void closeConnection(SerializationParams serialization) throws SQLException {
    Connection connection = newConnection(serialization);

    assertThat(stargateMeta.connections()).hasSize(1);

    connection.close();

    assertThat(stargateMeta.connections()).hasSize(0);
    assertThatThrownBy(connection::createStatement).hasMessage("Connection closed");
  }

  private void validateValue(
      SerializationParams ser,
      String column,
      Object expectedValue,
      SqlGetter<String> getByName,
      SqlGetter<Integer> getByPosition)
      throws SQLException {
    validateValue(ser, column, expectedValue, expectedValue, getByName, getByPosition);
  }

  private void validateValue(
      SerializationParams ser,
      String column,
      Object valueByName,
      Object valueByPosition,
      SqlGetter<String> getByName,
      SqlGetter<Integer> getByPosition)
      throws SQLException {
    withAnySelectFrom(table3).returning(ImmutableList.of(sampleValues(table3, false)));

    Connection connection = newConnection(ser);
    Statement statement = connection.createStatement();

    ResultSet rs1 =
        statement.executeQuery(String.format("select %s from test_ks.supported_types", column));
    assertThat(rs1.next()).isTrue();
    assertThat(getByName.from(rs1, column)).isEqualTo(valueByName);
    assertThat(getByPosition.from(rs1, 1)).isEqualTo(valueByPosition);

    ResultSet rs2 = statement.executeQuery("select * from test_ks.supported_types");
    assertThat(rs2.next()).isTrue();
    assertThat(getByName.from(rs2, column)).isEqualTo(valueByName);
  }

  private <V> void validateParameter(
      SerializationParams ser, String column, V value, SqlSetter<Integer, V> setter)
      throws SQLException {
    withAnySelectFrom(table3).returning(ImmutableList.of(sampleValues(table3, false)));
    withAnyInsertInfo(table3).returningNothing();
    withAnyUpdateOf(table3).returningNothing();

    Connection connection = newConnection(ser);

    String cql =
        String.format("update test_ks.supported_types set %s = ? where %s = ?", column, column);

    Column.ColumnType type = table3.column(column).type();
    assertThat(type).isNotNull();

    withQuery(table3, cql, "example", TypeUtils.jdbcToDriverValue(value, type)).returningNothing();
    PreparedStatement statement = connection.prepareStatement(cql);

    setter.set(statement, 1, value);
    setter.set(statement, 2, value);
    int updateCount = statement.executeUpdate();
    assertThat(updateCount).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("allColumnParams")
  public void allGetObject(SerializationParams ser, String column, Object value)
      throws SQLException {
    // Note: for some reason getObject(int) returns a value coerced to the target type, but
    // getObject(name)
    // returns the raw value from the protocol layer, which in case of JSON serialization is `int`
    // for all
    // small numbers, so we have to coerce our "expected" value to allow assertions to pass.
    // TODO: check for bugs in Avatica code
    validateValue(
        ser,
        column,
        ser.coerceClientValue(value),
        value,
        ResultSet::getObject,
        ResultSet::getObject);
  }

  @ParameterizedTest
  @MethodSource("allColumnParams")
  public void allSetObject(SerializationParams ser, String column, Object value)
      throws SQLException {
    // TODO: fix BigDecimal parameter serialization: C2-254, check for bugs in Avatica code
    Assumptions.assumeThat(column)
        .withFailMessage("Review setObject for %s", column)
        .isNotEqualTo("c_decimal");

    validateParameter(ser, column, value, PreparedStatement::setObject);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getAscii(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_ascii", "example", ResultSet::getString, ResultSet::getString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setAscii(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_ascii", "example", PreparedStatement::setString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getBigint(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_bigint", Long.MAX_VALUE, ResultSet::getLong, ResultSet::getLong);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setBigint(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_bigint", Long.MAX_VALUE, PreparedStatement::setLong);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getBoolean(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_boolean", false, ResultSet::getBoolean, ResultSet::getBoolean);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setBoolean(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_boolean", false, PreparedStatement::setBoolean);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getCounter(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_counter", Long.MAX_VALUE, ResultSet::getLong, ResultSet::getLong);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setCounter(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_counter", Long.MAX_VALUE, PreparedStatement::setLong);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getDate(SerializationParams ser) throws SQLException {
    validateValue(
        ser,
        "c_date",
        Date.valueOf(LocalDate.of(2020, 1, 2)),
        ResultSet::getDate,
        ResultSet::getDate);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setDate(SerializationParams ser) throws SQLException {
    validateParameter(
        ser, "c_date", Date.valueOf(LocalDate.of(2020, 1, 2)), PreparedStatement::setDate);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getDecimal(SerializationParams ser) throws SQLException {
    validateValue(
        ser, "c_decimal", BIG_DECIMAL_EXAMPLE, ResultSet::getBigDecimal, ResultSet::getBigDecimal);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setDecimal(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_decimal", BIG_DECIMAL_EXAMPLE, PreparedStatement::setBigDecimal);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getDouble(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_double", Double.MAX_VALUE, ResultSet::getDouble, ResultSet::getDouble);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setDouble(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_double", Double.MAX_VALUE, PreparedStatement::setDouble);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getDuration(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_duration", "P1M2DT3S", ResultSet::getString, ResultSet::getString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setDuration(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_duration", "P1M2DT3S", PreparedStatement::setString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getInet(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_inet", "127.0.0.1", ResultSet::getString, ResultSet::getString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setInet(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_inet", "127.0.0.1", PreparedStatement::setString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getInt(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_int", Integer.MAX_VALUE, ResultSet::getInt, ResultSet::getInt);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setInt(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_int", Integer.MAX_VALUE, PreparedStatement::setInt);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getSmallint(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_smallint", Short.MAX_VALUE, ResultSet::getShort, ResultSet::getShort);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setSmallint(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_smallint", Short.MAX_VALUE, PreparedStatement::setShort);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getText(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_text", "example", ResultSet::getString, ResultSet::getString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setText(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_text", "example", PreparedStatement::setString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getTime(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_time", Time.valueOf("23:42:11"), ResultSet::getTime, ResultSet::getTime);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setTime(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_time", Time.valueOf("23:42:11"), PreparedStatement::setTime);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getTimestamp(SerializationParams ser) throws SQLException {
    validateValue(
        ser, "c_timestamp", new Timestamp(0), ResultSet::getTimestamp, ResultSet::getTimestamp);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setTimestamp(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_timestamp", new Timestamp(0), PreparedStatement::setTimestamp);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getTimeuuid(SerializationParams ser) throws SQLException {
    validateValue(
        ser, "c_timeuuid", Uuids.startOf(1).toString(), ResultSet::getString, ResultSet::getString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setTimeuuid(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_timeuuid", Uuids.startOf(1).toString(), PreparedStatement::setString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getTinyint(SerializationParams ser) throws SQLException {
    validateValue(ser, "c_tinyint", Byte.MAX_VALUE, ResultSet::getByte, ResultSet::getByte);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setTinyint(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_tinyint", Byte.MAX_VALUE, PreparedStatement::setByte);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void getUuid(SerializationParams ser) throws SQLException {
    validateValue(
        ser, "c_uuid", Uuids.startOf(1).toString(), ResultSet::getString, ResultSet::getString);
  }

  @ParameterizedTest
  @EnumSource(SerializationParams.class)
  public void setUuid(SerializationParams ser) throws SQLException {
    validateParameter(ser, "c_uuid", Uuids.startOf(1).toString(), PreparedStatement::setString);
  }

  @FunctionalInterface
  private interface SqlGetter<T> {

    Object from(ResultSet rs, T column) throws SQLException;
  }

  @FunctionalInterface
  private interface SqlSetter<T, V> {

    void set(PreparedStatement statement, T column, V value) throws SQLException;
  }
}
