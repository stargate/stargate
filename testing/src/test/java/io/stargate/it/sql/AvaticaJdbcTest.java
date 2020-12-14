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
package io.stargate.it.sql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.storage.StargateConnectionInfo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.calcite.avatica.remote.Driver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE sql_test (x int, primary key (x))",
    })
public class AvaticaJdbcTest extends BaseOsgiIntegrationTest {

  @BeforeAll
  public static void loadJdbcDriver() throws ClassNotFoundException {
    Class.forName(Driver.class.getName()); // force the Driver class to initialize
  }

  @Test
  public void testBasicJdbcQuery(
      StargateConnectionInfo stargate, @TestKeyspace CqlIdentifier keyspaceId) throws SQLException {
    Connection c =
        DriverManager.getConnection(
            String.format(
                "jdbc:avatica:remote:url=http://%s:8765;serialization=%s",
                stargate.seedAddress(), Driver.Serialization.PROTOBUF.name()),
            "cassandra",
            "cassandra");

    PreparedStatement p =
        c.prepareStatement(String.format("select x from %s.sql_test", keyspaceId.asCql(false)));

    ResultSet rs = p.executeQuery();
    assertThat(rs.next()).isFalse();

    c.createStatement()
        .executeUpdate(
            String.format("insert into %s.sql_test (x) values (123)", keyspaceId.asCql(false)));

    rs = p.executeQuery();
    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt(1)).isEqualTo(123);
  }
}
