package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.KeycloakContainer;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.http.HttpStatus;
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
    })
public class GraphqlJWTAuthTest extends BaseOsgiIntegrationTest {

  private static String authToken;
  private static KeycloakContainer keycloakContainer;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String host;

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
    host = "http://" + cluster.seedAddress();

    authToken = keycloakContainer.generateJWT();
  }

  @Test
  @DisplayName("Should execute GraphQL mutation when authorized")
  public void mutationTest(@TestKeyspace CqlIdentifier keyspaceId) {
    Map<String, Object> response =
        getGraphqlData(
            keyspaceId,
            "mutation {\n"
                + "  insertBooks(value: {title:\"Moby Dick\", author:\"Herman Melville\"}) {\n"
                + "    value { title }\n"
                + "  }\n"
                + "}");
    assertThat(response).isNotNull();
    assertThat(response.get("insertBooks"))
        .asInstanceOf(MAP)
        .extractingByKey("value")
        .asInstanceOf(MAP)
        .extractingByKey("title")
        .isEqualTo("Moby Dick");
  }

  @Test
  @DisplayName("Should execute batch of GraphQL mutations when authorized")
  public void batchTest(@TestKeyspace CqlIdentifier keyspaceId) {
    Map<String, Object> response =
        getGraphqlData(
            keyspaceId,
            "mutation {\n"
                + "  moby: insertBooks(value: {title:\"Moby Dick\", author:\"Herman Melville\"}) {\n"
                + "    value { title }\n"
                + "  }\n"
                + "  catch22: insertBooks(value: {title:\"Catch-22\", author:\"Joseph Heller\"}) {\n"
                + "    value { title }\n"
                + "  }\n"
                + "}\n");
    assertThat(response).isNotNull();
    assertThat(response.get("moby"))
        .asInstanceOf(MAP)
        .extractingByKey("value")
        .asInstanceOf(MAP)
        .extractingByKey("title")
        .isEqualTo("Moby Dick");
    assertThat(response.get("catch22"))
        .asInstanceOf(MAP)
        .extractingByKey("value")
        .asInstanceOf(MAP)
        .extractingByKey("title")
        .isEqualTo("Catch-22");
  }

  @Test
  @DisplayName("Should fail to execute GraphQL when not authorized")
  public void unauthorizedTest(@TestKeyspace CqlIdentifier keyspaceId) {
    String error =
        getGraphqlError(keyspaceId, "mutation { insertSecret(value: {k:1}) { value { k } } }");
    assertThat(error).contains("User web_user has no MODIFY permission");
  }

  private Map<String, Object> getGraphqlData(CqlIdentifier keyspaceId, String query) {
    Map<String, Object> response = getGraphqlResponse(keyspaceId, query);
    assertThat(response).isNotNull();
    assertThat(response.get("errors")).isNull();
    @SuppressWarnings("unchecked")
    Map<String, Object> data = (Map<String, Object>) response.get("data");
    return data;
  }

  private String getGraphqlError(CqlIdentifier keyspaceId, String query) {
    Map<String, Object> response = getGraphqlResponse(keyspaceId, query);
    assertThat(response).isNotNull();
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> errors = (List<Map<String, Object>>) response.get("errors");
    assertThat(errors).hasSize(1);
    return (String) errors.get(0).get("message");
  }

  private Map<String, Object> getGraphqlResponse(CqlIdentifier keyspaceId, String query) {
    try {
      OkHttpClient okHttpClient = getHttpClient();
      String url = String.format("%s:8080/graphql/%s", host, keyspaceId.asInternal());
      Map<String, Object> formData = new HashMap<>();
      formData.put("query", query);

      MediaType JSON = MediaType.parse("application/json; charset=utf-8");
      okhttp3.Response response =
          okHttpClient
              .newCall(
                  new Request.Builder()
                      .post(RequestBody.create(JSON, OBJECT_MAPPER.writeValueAsBytes(formData)))
                      .url(url)
                      .build())
              .execute();
      assertThat(response.body()).isNotNull();
      String bodyString = response.body().string();
      assertThat(response.code())
          .as("Unexpected error %d: %s", response.code(), bodyString)
          .isEqualTo(HttpStatus.SC_OK);
      @SuppressWarnings("unchecked")
      Map<String, Object> graphqlResponse = OBJECT_MAPPER.readValue(bodyString, Map.class);
      return graphqlResponse;
    } catch (IOException e) {
      fail("Unexpected error while sending POST request", e);
      return null; // never reached
    }
  }

  private OkHttpClient getHttpClient() {
    return new OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(180))
        .callTimeout(Duration.ofSeconds(180))
        .readTimeout(Duration.ofSeconds(180))
        .writeTimeout(Duration.ofSeconds(180))
        .addInterceptor(
            chain ->
                chain.proceed(
                    chain.request().newBuilder().addHeader("X-Cassandra-Token", authToken).build()))
        .build();
  }
}
