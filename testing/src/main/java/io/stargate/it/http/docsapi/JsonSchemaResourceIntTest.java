package io.stargate.it.http.docsapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@NotThreadSafe
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec()
public class JsonSchemaResourceIntTest extends BaseOsgiIntegrationTest {
  private static final String TARGET_COLLECTION = "collection";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final OkHttpClient CLIENT =
      new OkHttpClient().newBuilder().readTimeout(Duration.ofMinutes(3)).build();

  private static String authToken;
  private static String host;
  private static String hostWithPort;
  private static String keyspace;
  public static String collectionPath;

  @BeforeAll
  public static void setup(
      StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceIdentifier)
      throws IOException {
    host = "http://" + cluster.seedAddress();
    hostWithPort = host + ":8082";
    keyspace = keyspaceIdentifier.toString();
    collectionPath =
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/" + TARGET_COLLECTION;
    initAuth();
  }

  @AfterEach
  public void emptyTargetCollection(CqlSession session) {
    ResultSet results = session.execute(String.format("SELECT key FROM %s", TARGET_COLLECTION));
    List<String> ids = new ArrayList<>();
    results.forEach(row -> ids.add(String.format("'%s'", row.getString("key"))));

    // This resets the JSON schema on the table
    session.execute(String.format("ALTER TABLE %s WITH COMMENT = ''", TARGET_COLLECTION));

    if (!ids.isEmpty()) {
      session.execute(
          String.format(
              "DELETE FROM %s WHERE key IN (%s)", TARGET_COLLECTION, String.join(", ", ids)));
    }
  }

  private static void initAuth() throws IOException {
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    RequestBody requestBody =
        RequestBody.create(
            MediaType.parse("application/json"),
            OBJECT_MAPPER.writeValueAsString(new Credentials("cassandra", "cassandra")));

    Request request =
        new Request.Builder()
            .url(String.format("%s:8081/v1/auth/token/generate", host))
            .post(requestBody)
            .addHeader("X-Cassandra-Request-Id", "foo")
            .build();
    Response response = CLIENT.newCall(request).execute();
    ResponseBody body = response.body();

    assertThat(body).isNotNull();
    AuthTokenResponse authTokenResponse =
        OBJECT_MAPPER.readValue(body.string(), AuthTokenResponse.class);
    authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();
  }

  @Test
  public void testIt() throws IOException {
    RestUtils.post(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections",
        "{\"name\":\"collection\"}",
        201);
    JsonNode schema =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("schema.json"));
    RestUtils.put(authToken, collectionPath + "/json-schema", schema.toString(), 200);

    String schemaResp = RestUtils.get(authToken, collectionPath + "/json-schema", 200);
    assertThat(schema.toString()).isEqualTo(OBJECT_MAPPER.readTree(schemaResp).requiredAt("/schema").toString());

    JsonNode obj = OBJECT_MAPPER.readTree("{\"id\":1, \"name\":\"a\", \"price\":1}");
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);

    obj = OBJECT_MAPPER.readTree("{\"id\":1, \"price\":1}");
    String resp = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 400);
    JsonNode data = OBJECT_MAPPER.readTree(resp);
    assertThat(data.requiredAt("/description"))
        .isEqualTo(
            TextNode.valueOf(
                "Invalid JSON: [object has missing required properties ([\\\"name\\\"])]"));

    obj = OBJECT_MAPPER.readTree("{\"id\":1, \"name\":\"a\", \"price\":-1}");
    resp = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 400);
    data = OBJECT_MAPPER.readTree(resp);
    assertThat(data.requiredAt("/description"))
        .isEqualTo(
            TextNode.valueOf(
                "Invalid JSON: [numeric instance is lower than the required minimum (minimum: 0, found: -1)]"));
  }

  @Test
  public void testInvalidSchema() throws IOException {
    JsonNode schema =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("invalid-schema.json"));
    String resp = RestUtils.put(authToken, collectionPath + "/json-schema", schema.toString(), 400);
    JsonNode data = OBJECT_MAPPER.readTree(resp);
    assertThat(data.requiredAt("/description"))
        .isEqualTo(TextNode.valueOf("The provided JSON schema is invalid or malformed."));
  }
}
