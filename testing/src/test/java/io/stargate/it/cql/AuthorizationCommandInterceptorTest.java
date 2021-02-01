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
package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.ClusterConnectionInfo;
import io.stargate.it.storage.LogCollector;
import io.stargate.it.storage.StargateLogExtension;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import io.stargate.testing.TestingServicesActivator;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    createKeyspace = false,
    dropKeyspace = false,
    initQueries = {
      "CREATE ROLE IF NOT EXISTS 'auth_user1'",
      "CREATE KEYSPACE IF NOT EXISTS auth_keyspace2 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}",
      "CREATE TABLE IF NOT EXISTS auth_keyspace2.table3 (userid text, PRIMARY KEY (userid))",
    })
@ExtendWith(StargateLogExtension.class)
public class AuthorizationCommandInterceptorTest extends BaseOsgiIntegrationTest {
  private static CqlSession session;
  private LogCollector log;

  @BeforeAll
  public static void checkDse(ClusterConnectionInfo backend) {
    assumeTrue(backend.isDse()); // TODO: Support C*
  }

  @BeforeAll
  public static void buildSession(CqlSessionBuilder builder) {
    session = builder.withKeyspace("auth_keyspace2").build();
  }

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.enableAuth(true);
    builder.putSystemProperties(
        TestingServicesActivator.AUTHZ_PROCESSOR_PROPERTY,
        TestingServicesActivator.LOGGING_AUTHZ_PROCESSOR_ID);
  }

  @BeforeEach
  public void setupLogInterceptor(LogCollector log) {
    this.log = log;
  }

  private List<String> addedMsgs(String cql) {
    session.execute(cql);
    return log.filter(0, Pattern.compile(".+testing: addPermissions: (.+)"), 1);
  }

  private List<String> removedMsgs(String cql) {
    session.execute(cql);
    return log.filter(0, Pattern.compile(".+testing: removePermissions: (.+)"), 1);
  }

  @Test
  public void grantSelect() {
    assertThat(addedMsgs("GRANT SELECT on auth_keyspace2.table3 TO 'auth_user1'"))
        .containsExactly(
            "cassandra, ALLOW, ACCESS, [SELECT], AuthorizedResource{kind=TABLE, keyspace=auth_keyspace2, element=table3}, auth_user1");
  }

  @Test
  public void revokeSelect() {
    assertThat(removedMsgs("REVOKE SELECT on table3 FROM 'auth_user1'"))
        .containsExactly(
            "cassandra, ALLOW, ACCESS, [SELECT], AuthorizedResource{kind=TABLE, keyspace=auth_keyspace2, element=table3}, auth_user1");
  }

  @Test
  public void grantDescribeAllKeyspaces() {
    assertThat(addedMsgs("GRANT DESCRIBE ON ALL KEYSPACES TO 'auth_user1'"))
        .containsExactly(
            "cassandra, ALLOW, ACCESS, [DESCRIBE], AuthorizedResource{kind=TABLE, keyspace=*, element=*}, auth_user1");
  }

  @Test
  public void grantTruncateAllTables() {
    assertThat(addedMsgs("GRANT TRUNCATE ON ALL TABLES IN KEYSPACE auth_keyspace2 TO 'auth_user1'"))
        .containsExactly(
            "cassandra, ALLOW, ACCESS, [TRUNCATE], AuthorizedResource{kind=TABLE, keyspace=auth_keyspace2, element=*}, auth_user1");
  }

  @Test
  public void grantExecuteAllFunctions() {
    assertThat(addedMsgs("GRANT EXECUTE ON ALL FUNCTIONS TO auth_user1"))
        .containsExactly(
            "cassandra, ALLOW, ACCESS, [EXECUTE], AuthorizedResource{kind=FUNCTION, keyspace=*, element=*}, auth_user1");
  }

  @Test
  public void grantExecuteAllFunctionsInKeyspace() {
    assertThat(addedMsgs("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE auth_keyspace2 TO auth_user1"))
        .containsExactly(
            "cassandra, ALLOW, ACCESS, [EXECUTE], AuthorizedResource{kind=FUNCTION, keyspace=auth_keyspace2, element=*}, auth_user1");
  }

  @Test
  public void restrictUpdate() {
    assertThat(addedMsgs("RESTRICT UPDATE on table3 TO 'auth_user1'"))
        .containsExactly(
            "cassandra, DENY, ACCESS, [UPDATE], AuthorizedResource{kind=TABLE, keyspace=auth_keyspace2, element=table3}, auth_user1");
  }

  @Test
  public void unrestrictUpdate() {
    assertThat(removedMsgs("UNRESTRICT UPDATE on auth_keyspace2.table3 FROM 'auth_user1'"))
        .containsExactly(
            "cassandra, DENY, ACCESS, [UPDATE], AuthorizedResource{kind=TABLE, keyspace=auth_keyspace2, element=table3}, auth_user1");
  }

  @Test
  public void authorizeTruncate() {
    assertThat(addedMsgs("GRANT AUTHORIZE FOR SELECT, TRUNCATE on table3 TO 'auth_user1'"))
        .containsExactly(
            "cassandra, ALLOW, AUTHORITY, [SELECT, TRUNCATE], AuthorizedResource{kind=TABLE, keyspace=auth_keyspace2, element=table3}, auth_user1");
  }

  @Test
  public void revokeAuthorizeTruncate() {
    assertThat(removedMsgs("REVOKE AUTHORIZE FOR SELECT, TRUNCATE on table3 FROM 'auth_user1'"))
        .containsExactly(
            "cassandra, ALLOW, AUTHORITY, [SELECT, TRUNCATE], AuthorizedResource{kind=TABLE, keyspace=auth_keyspace2, element=table3}, auth_user1");
  }
}
