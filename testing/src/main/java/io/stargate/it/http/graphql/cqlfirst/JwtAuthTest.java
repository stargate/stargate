package io.stargate.it.http.graphql.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.KeycloakContainer;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE ROLE IF NOT EXISTS 'web_user' WITH PASSWORD = 'web_user' AND LOGIN = TRUE",
      "CREATE TABLE \"Books\"(title text PRIMARY KEY, author text)",
      "CREATE TABLE \"Secret\"(k int PRIMARY KEY)",
      "GRANT MODIFY ON TABLE \"Books\" TO web_user",
      "CREATE KEYSPACE IF NOT EXISTS stargate_graphql WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
      "GRANT SELECT ON KEYSPACE stargate_graphql TO web_user",
    })
public class JwtAuthTest extends BaseIntegrationTest {

  private static KeycloakContainer keycloakContainer;

  private CqlFirstClient client;

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) throws IOException {
    keycloakContainer = new KeycloakContainer();
    keycloakContainer.initKeycloakContainer();

    builder.enableAuth(true);
    builder.putSystemProperties("stargate.auth_id", "AuthJwtService");
    builder.putSystemProperties(
        "stargate.auth.jwt_provider_url",
        String.format(
            "%s/auth/realms/stargate/protocol/openid-connect/certs", keycloakContainer.host()));
  }

  @AfterAll
  public static void teardown() {
    keycloakContainer.stop();
  }

  @BeforeEach
  public void setup(StargateConnectionInfo cluster) throws IOException {
    client = new CqlFirstClient(cluster.seedAddress(), keycloakContainer.generateJWT());
  }

  @Test
  @DisplayName("Should execute GraphQL mutation when authorized")
  public void mutationTest(@TestKeyspace CqlIdentifier keyspaceId) {
    Map<String, Object> response =
        client.executeDmlQuery(
            keyspaceId,
            "mutation {\n"
                + "  insertBooks(value: {title:\"Moby Dick\", author:\"Herman Melville\"}) {\n"
                + "    value { title }\n"
                + "  }\n"
                + "}");
    assertThat(JsonPath.<String>read(response, "$.insertBooks.value.title")).isEqualTo("Moby Dick");
  }

  @Test
  @DisplayName("Should execute batch of GraphQL mutations when authorized")
  public void batchTest(@TestKeyspace CqlIdentifier keyspaceId) {
    Map<String, Object> response =
        client.executeDmlQuery(
            keyspaceId,
            "mutation {\n"
                + "  moby: insertBooks(value: {title:\"Moby Dick\", author:\"Herman Melville\"}) {\n"
                + "    value { title }\n"
                + "  }\n"
                + "  catch22: insertBooks(value: {title:\"Catch-22\", author:\"Joseph Heller\"}) {\n"
                + "    value { title }\n"
                + "  }\n"
                + "}\n");
    assertThat(JsonPath.<String>read(response, "$.moby.value.title")).isEqualTo("Moby Dick");
    assertThat(JsonPath.<String>read(response, "$.catch22.value.title")).isEqualTo("Catch-22");
  }

  @Test
  @DisplayName("Should fail to execute GraphQL when not authorized")
  public void unauthorizedTest(@TestKeyspace CqlIdentifier keyspaceId) {
    String error =
        client.getDmlQueryError(
            keyspaceId, "mutation { insertSecret(value: {k:1}) { value { k } } }");
    // Don't rely on the full message because it's not standardized across Cassandra/DSE versions
    assertThat(error).contains("UnauthorizedException");
  }
}
