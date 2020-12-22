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

import com.datastax.oss.driver.api.core.CqlSession;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.LogCollector;
import io.stargate.it.storage.StargateLogExtension;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.io.IOException;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE ROLE IF NOT EXISTS 'auth_user1'",
      "CREATE KEYSPACE IF NOT EXISTS auth_keyspace2 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}",
      "CREATE TABLE IF NOT EXISTS auth_keyspace2.table3 (userid text, PRIMARY KEY (userid))",
    })
@ExtendWith(StargateLogExtension.class)
public class AuthorizationProcessorTest extends BaseOsgiIntegrationTest {

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) throws IOException {
    builder.enableAuth(true);
    builder.putSystemProperties("stargate.authorization.processor.id", "TestAuthzProcessor");
  }

  @Test
  public void grantSelect(LogCollector log, CqlSession session) {
    session.execute("GRANT SELECT on auth_keyspace2.table3 TO 'auth_user1'");
    assertThat(log.filter(0, Pattern.compile(".+testing: addPermissions: (.+)"), 1))
        .contains("Actor{roleName=cassandra}, ALLOW, ACCESS, [AccessPermission{name=SELECT}]");
  }
}
