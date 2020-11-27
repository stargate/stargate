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
package io.stargate.api.sql.server.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.ImmutableList;
import io.stargate.api.sql.schema.TypeUtils;
import io.stargate.db.schema.Column;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PGServerAllTypesJdbcIT extends PGServerTestBase {

  private Connection openConnection() throws SQLException {
    return DriverManager.getConnection(
        String.format("jdbc:postgresql://localhost:%d/test", TEST_PORT));
  }

  public static Stream<Arguments> allColumnParams() {
    return sampleValues(table3, true).entrySet().stream()
        .filter(e -> e.getKey().startsWith("c_"))
        .map(e -> Arguments.of(e.getKey(), e.getValue()));
  }

  private void validateValue(
      String column,
      Object expectedValue,
      SqlGetter<String> getByName,
      SqlGetter<Integer> getByPosition)
      throws SQLException {
    validateValue(column, expectedValue, expectedValue, getByName, getByPosition);
  }

  private void validateValue(
      String column,
      Object valueByName,
      Object valueByPosition,
      SqlGetter<String> getByName,
      SqlGetter<Integer> getByPosition)
      throws SQLException {
    withAnySelectFrom(table3).returning(ImmutableList.of(sampleValues(table3, false)));

    Connection connection = openConnection();
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

  private <V> void validateParameter(String column, V value, SqlSetter<Integer, V> setter)
      throws SQLException {
    withAnySelectFrom(table3).returning(ImmutableList.of(sampleValues(table3, false)));
    withAnyInsertInfo(table3).returningNothing();
    withAnyUpdateOf(table3).returningNothing();

    Connection connection = openConnection();

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
  public void allGetObject(String column, Object value) throws SQLException {
    // Note: we return tinyint values as short because the PostgreSQL driver treats char values
    // as strings
    if (value instanceof Byte) {
      value = ((Byte) value).shortValue();
    }

    // Note: for some reason the PostgreSQL driver converts short values to int in getObject(...).
    if (value instanceof Short) {
      value = ((Short) value).intValue();
    }

    validateValue(column, value, value, ResultSet::getObject, ResultSet::getObject);
  }

  //  @ParameterizedTest
  //  @MethodSource("allColumnParams")
  //  public void allSetObject(String column, Object value)
  //      throws SQLException {
  //    validateParameter(column, value, PreparedStatement::setObject);
  //  }

  @Test
  public void getAscii() throws SQLException {
    validateValue("c_ascii", "example", ResultSet::getString, ResultSet::getString);
  }

  @Test
  public void getBigint() throws SQLException {
    validateValue("c_bigint", Long.MAX_VALUE, ResultSet::getLong, ResultSet::getLong);
  }

  @Test
  public void getBoolean() throws SQLException {
    validateValue("c_boolean", false, ResultSet::getBoolean, ResultSet::getBoolean);
  }

  @Test
  public void getCounter() throws SQLException {
    validateValue("c_counter", Long.MAX_VALUE, ResultSet::getLong, ResultSet::getLong);
  }

  @Test
  public void getDate() throws SQLException {
    validateValue(
        "c_date", Date.valueOf(LocalDate.of(2020, 1, 2)), ResultSet::getDate, ResultSet::getDate);
  }

  @Test
  public void getDecimal() throws SQLException {
    validateValue(
        "c_decimal", BIG_DECIMAL_EXAMPLE, ResultSet::getBigDecimal, ResultSet::getBigDecimal);
  }

  @Test
  public void getDouble() throws SQLException {
    validateValue("c_double", Double.MAX_VALUE, ResultSet::getDouble, ResultSet::getDouble);
  }

  @Test
  public void getDuration() throws SQLException {
    validateValue("c_duration", "P1M2DT3S", ResultSet::getString, ResultSet::getString);
  }

  @Test
  public void getInet() throws SQLException {
    validateValue("c_inet", "127.0.0.1", ResultSet::getString, ResultSet::getString);
  }

  @Test
  public void getInt() throws SQLException {
    validateValue("c_int", Integer.MAX_VALUE, ResultSet::getInt, ResultSet::getInt);
  }

  @Test
  public void getSmallint() throws SQLException {
    validateValue("c_smallint", Short.MAX_VALUE, ResultSet::getShort, ResultSet::getShort);
  }

  @Test
  public void getText() throws SQLException {
    validateValue("c_text", "example", ResultSet::getString, ResultSet::getString);
  }

  @Test
  public void getTime() throws SQLException {
    validateValue("c_time", Time.valueOf("23:42:11"), ResultSet::getTime, ResultSet::getTime);
  }

  @Test
  public void getTimestamp() throws SQLException {
    validateValue(
        "c_timestamp", new Timestamp(0), ResultSet::getTimestamp, ResultSet::getTimestamp);
  }

  @Test
  public void getTimeuuid() throws SQLException {
    validateValue(
        "c_timeuuid", Uuids.startOf(1).toString(), ResultSet::getString, ResultSet::getString);
  }

  @Test
  public void getTinyint() throws SQLException {
    validateValue("c_tinyint", Byte.MAX_VALUE, ResultSet::getByte, ResultSet::getByte);
  }

  @Test
  public void getUuid() throws SQLException {
    validateValue(
        "c_uuid", Uuids.startOf(1).toString(), ResultSet::getString, ResultSet::getString);
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
