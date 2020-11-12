package io.stargate.it.http.docsapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.ClusterConnectionInfo;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
@NotThreadSafe
public class CollectionTest extends BaseOsgiIntegrationTest {

  private String keyspace;
  private CqlSession session;
  private boolean isDse;
  private static String authToken;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final OkHttpClient client =
      new OkHttpClient().newBuilder().readTimeout(3, TimeUnit.MINUTES).build();

  private String host;
  private String hostWithPort;

  @BeforeEach
  public void setup(ClusterConnectionInfo backend, StargateConnectionInfo stargate)
      throws IOException {
    host = "http://" + stargate.seedAddress();
    hostWithPort = host + ":8082";

    keyspace = "ks_collection_" + System.currentTimeMillis();
    isDse = backend.isDse();
    session =
        CqlSession.builder()
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(5))
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(180))
                    .withDuration(
                        DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT,
                        Duration.ofSeconds(180))
                    .withDuration(
                        DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(180))
                    .withDuration(
                        DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(180))
                    .build())
            .withAuthCredentials("cassandra", "cassandra")
            .addContactPoint(new InetSocketAddress(stargate.seedAddress(), 9043))
            .withLocalDatacenter(stargate.datacenter())
            .build();

    assertThat(
            session
                .execute(
                    String.format(
                        "create keyspace if not exists %s WITH replication = "
                            + "{'class': 'SimpleStrategy', 'replication_factor': 1 }",
                        keyspace))
                .wasApplied())
        .isTrue();

    initAuth();
  }

  private void initAuth() throws IOException {
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    RequestBody requestBody =
        RequestBody.create(
            MediaType.parse("application/json"),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")));

    Request request =
        new Request.Builder()
            .url(String.format("%s:8081/v1/auth/token/generate", host))
            .post(requestBody)
            .addHeader("X-Cassandra-Request-Id", "foo")
            .build();
    Response response = client.newCall(request).execute();
    ResponseBody body = response.body();

    assertThat(body).isNotNull();
    AuthTokenResponse authTokenResponse =
        objectMapper.readValue(body.string(), AuthTokenResponse.class);
    authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();
  }

  @AfterEach
  public void teardown() {
    session.close();
  }

  @Test
  public void testGet() throws IOException {
    String r =
        RestUtils.get(
            authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections?raw=false", 200);
    String expected = "{\"data\": []}";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(expected));

    r =
        RestUtils.get(
            authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections?raw=true", 200);
    expected = "[]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(expected));
  }

  @Test
  public void testPost() throws IOException {
    String r =
        RestUtils.get(
            authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections?raw=true", 200);
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree("[]"));

    // Create a brand new collection
    String newColl = "{\"name\": \"newcollection\"}";
    RestUtils.post(
        authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections", newColl, 201);

    r =
        RestUtils.get(
            authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections?raw=true", 200);
    String expected = "[{\"name\": \"newcollection\", \"upgradeAvailable\": false}]";

    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(expected));
  }

  @Test
  public void testInvalidPost() throws IOException {
    String r =
        RestUtils.get(
            authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections?raw=true", 200);
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree("[]"));

    // Create a brand new collection
    String newColl = "{}";
    r =
        RestUtils.post(
            authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections", newColl, 400);
    assertThat(r).contains("`name` is required to create a collection");
  }

  @Test
  public void testDelete() throws IOException {
    String r =
        RestUtils.get(
            authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections?raw=true", 200);
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree("[]"));

    // Create a brand new collection
    String newColl = "{\"name\": \"newcollection\"}";
    RestUtils.post(
        authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections", newColl, 201);

    // Delete it
    RestUtils.delete(
        authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/newcollection", 204);

    // Delete it again, not found
    RestUtils.delete(
        authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/newcollection", 404);
  }

  @Test
  public void testUpgrade() throws IOException {
    if (isDse) {
      // Create a brand new collection, it should already have SAI so it requires no upgrade
      String newColl = "{\"name\": \"newcollection\"}";
      RestUtils.post(
          authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections", newColl, 201);

      // Illegal, as the collection is already in its most upgraded state (with SAI)
      String upgradeAction = "{\"upgradeType\": \"SAI_INDEX_UPGRADE\"}";
      String r =
          RestUtils.post(
              authToken,
              hostWithPort + "/v2/namespaces/" + keyspace + "/collections/newcollection/upgrade",
              upgradeAction,
              400);
      assertThat(r).isEqualTo("That collection cannot be upgraded in that manner");

      // Drop all the relevant indexes to simulate "downgrading"
      dropIndexes("newcollection");

      // Now do the upgrade to add SAI
      r =
          RestUtils.post(
              authToken,
              hostWithPort
                  + "/v2/namespaces/"
                  + keyspace
                  + "/collections/newcollection/upgrade?raw=true",
              upgradeAction,
              200);
      String expected = "{\"name\":\"newcollection\",\"upgradeAvailable\":false}";
      assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(expected));
    }
  }

  private void dropIndexes(String collection) {
    session.execute(String.format("DROP INDEX \"%s\".\"%s\"", keyspace, collection + "_leaf_idx"));

    session.execute(
        String.format("DROP INDEX \"%s\".\"%s\"", keyspace, collection + "_text_value_idx"));

    session.execute(
        String.format("DROP INDEX \"%s\".\"%s\"", keyspace, collection + "_dbl_value_idx"));

    session.execute(
        String.format("DROP INDEX \"%s\".\"%s\"", keyspace, collection + "_bool_value_idx"));
  }
}
