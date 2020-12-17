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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

class PGServerJdbcIT extends PGServerTestBase {

  private Connection openConnection() throws SQLException {
    return DriverManager.getConnection(
        String.format("jdbc:postgresql://localhost:%d/test", TEST_PORT));
  }

  @Test
  public void connect() throws SQLException {
    Connection connection = openConnection();
    assertThat(connection).isNotNull();

    assertThat((Throwable) connection.getWarnings())
        .hasMessageContaining("Stargate")
        .hasMessageContaining("experimental");
  }

  @Test
  public void emptySelect() throws SQLException {
    withQuery(table1, "SELECT a FROM test_ks.test1").returningNothing();

    Connection connection = openConnection();
    assertThat(connection).isNotNull();

    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery("select * from test_ks.test1");
    assertThat(rs.next()).isFalse();
  }

  @Test
  public void simpleSelect() throws SQLException {
    withQuery(table1, "SELECT a FROM test_ks.test1")
        .returning(ImmutableList.of(ImmutableMap.of("a", 11), ImmutableMap.of("a", 22)));

    Connection connection = openConnection();
    assertThat(connection).isNotNull();

    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery("select * from test_ks.test1");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt(1)).isEqualTo(11);
    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt(1)).isEqualTo(22);
    assertThat(rs.next()).isFalse();
  }

  @Test
  public void simpleConsecutiveSelects() throws SQLException {
    withQuery(table1, "SELECT a FROM test_ks.test1")
        .returning(ImmutableList.of(ImmutableMap.of("a", 11), ImmutableMap.of("a", 22)));

    Connection connection = openConnection();
    assertThat(connection).isNotNull();

    PreparedStatement statement = connection.prepareStatement("select * from test_ks.test1");

    for (int i = 0; i < 3; i++) {
      ResultSet rs = statement.executeQuery();

      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt(1)).isEqualTo(11);
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt(1)).isEqualTo(22);
      assertThat(rs.next()).isFalse();

      rs.close();
    }

    statement.close();
    connection.close();
  }

  @Test
  public void simpleInsertWithPlaceholder() throws SQLException {
    withAnyInsertInfo(table2).returningNothing();

    Connection connection = openConnection();
    assertThat(connection).isNotNull();

    PreparedStatement statement =
        connection.prepareStatement("insert into test_ks.test2 (x, y) values (?, ?)");
    statement.setInt(1, 123);
    statement.setString(2, "abc");
    assertThat(statement.executeUpdate()).isEqualTo(1);
  }

  @Test
  public void setInPreparedStatement() throws SQLException {
    Connection connection = openConnection();
    PreparedStatement prepared = connection.prepareStatement("set application_name = 'test123'");
    assertThat(prepared.execute()).isFalse();
  }
}
