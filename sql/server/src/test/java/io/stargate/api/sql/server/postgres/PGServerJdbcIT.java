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
import io.stargate.api.sql.AbstractDataStoreTest;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.datastore.DataStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PGServerJdbcIT extends AbstractDataStoreTest {

  private static final int TEST_PORT = 5432;
  private static final AtomicReference<DataStore> dataStoreRef = new AtomicReference<>();

  private static PGServer server;

  @BeforeAll
  public static void startServer() {
    AuthenticationService authenticator = Mockito.mock(AuthenticationService.class);
    server = new PGServer(dataStoreRef::get, authenticator, TEST_PORT);
    server.start();
  }

  @AfterAll
  public static void stopServer() throws ExecutionException, InterruptedException {
    server.stop();
  }

  @BeforeEach
  public void setDataStore() {
    dataStoreRef.set(dataStore);
  }

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
}
