package io.stargate.it.http.docsapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@NotThreadSafe
public class NamespaceResourceIntTest extends BaseIntegrationTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final OkHttpClient CLIENT =
      new OkHttpClient().newBuilder().readTimeout(Duration.ofMinutes(3)).build();

  private static String authToken;
  private static String host;
  private static String hostWithPort;
  private static String basePath;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) throws IOException {
    host = "http://" + cluster.seedAddress();
    hostWithPort = host + ":8082";
    basePath = String.format("%s/v2/schemas/namespaces", hostWithPort);

    initAuth();
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

  // Below are "namespace" tests that should match the REST Api's keyspace tests
  // plus some additional ones for the validation

  @Nested
  @ExtendWith(CqlSessionExtension.class)
  @CqlSessionSpec(createKeyspace = false, createSession = false)
  class GetAllNamespaces {

    @Test
    public void notRaw() throws IOException {
      String body = RestUtils.get(authToken, basePath, HttpStatus.SC_OK);

      JsonNode json = OBJECT_MAPPER.readTree(body);
      assertThat(json.get("data"))
          .isNotNull()
          .isInstanceOfSatisfying(
              ArrayNode.class,
              dataArray ->
                  assertThat(dataArray)
                      .anySatisfy(
                          namespace -> {
                            assertThat(namespace.findValuesAsText("name")).containsOnly("system");
                            assertThat(namespace.findValues("datacenters")).isNullOrEmpty();
                          }));
    }

    @Test
    public void raw() throws IOException {
      String body = RestUtils.get(authToken, basePath + "?raw=true", HttpStatus.SC_OK);

      JsonNode json = OBJECT_MAPPER.readTree(body);
      assertThat(json)
          .isInstanceOfSatisfying(
              ArrayNode.class,
              dataArray ->
                  assertThat(dataArray)
                      .anySatisfy(
                          namespace -> {
                            assertThat(namespace.findValuesAsText("name"))
                                .containsOnly("system_schema");
                            assertThat(namespace.findValues("datacenters")).isNullOrEmpty();
                          }));
    }

    @Test
    public void missingToken() throws IOException {
      RestUtils.get(null, basePath, HttpStatus.SC_UNAUTHORIZED);
    }

    @Test
    public void badToken() throws IOException {
      RestUtils.get("foo", basePath, HttpStatus.SC_UNAUTHORIZED);
    }
  }

  @Nested
  @ExtendWith(CqlSessionExtension.class)
  @CqlSessionSpec()
  class GetOneNamespace {

    @Test
    public void notRaw(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String body = RestUtils.get(authToken, basePath + "/" + keyspace, HttpStatus.SC_OK);

      JsonNode json = OBJECT_MAPPER.readTree(body);
      assertThat(json.get("data"))
          .isInstanceOfSatisfying(
              ObjectNode.class,
              namespace -> {
                assertThat(namespace.findValuesAsText("name")).containsOnly(keyspace.toString());
                assertThat(namespace.findValues("datacenters")).isNullOrEmpty();
              });
    }

    @Test
    public void raw(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String body =
          RestUtils.get(authToken, basePath + "/" + keyspace + "?raw=true", HttpStatus.SC_OK);

      JsonNode json = OBJECT_MAPPER.readTree(body);
      assertThat(json)
          .isInstanceOfSatisfying(
              ObjectNode.class,
              namespace -> {
                assertThat(namespace.findValuesAsText("name")).containsOnly(keyspace.toString());
                assertThat(namespace.findValues("datacenters")).isNullOrEmpty();
              });
    }

    @Test
    public void notExisting() throws IOException {
      RestUtils.get(authToken, basePath + "/not_there", HttpStatus.SC_NOT_FOUND);
    }
  }

  @Nested
  @ExtendWith(CqlSessionExtension.class)
  @CqlSessionSpec(createKeyspace = false, createSession = false)
  class CreateNamespace {

    @Test
    public void simpleStrategy() throws IOException {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String payload = String.format("{\"name\": \"%s\", \"replicas\": 1}", keyspace);

      String result = RestUtils.post(authToken, basePath, payload, HttpStatus.SC_CREATED);

      assertThat(result).isEqualTo(String.format("{\"name\":\"%s\"}", keyspace));
      // validate with get as well
      String getResult =
          RestUtils.get(authToken, basePath + "/" + keyspace + "?raw=true", HttpStatus.SC_OK);
      JsonNode json = OBJECT_MAPPER.readTree(getResult);
      assertThat(json)
          .isInstanceOfSatisfying(
              ObjectNode.class,
              namespace -> {
                assertThat(namespace.findValuesAsText("name")).containsOnly(keyspace);
                assertThat(namespace.findValues("datacenters")).isNullOrEmpty();
              });
    }

    @Test
    public void networkTopologyStrategy() throws IOException {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String payload =
          String.format(
              "{\"name\": \"%s\", \"datacenters\": [ { \"name\": \"dc1\", \"replicas\": 1 } ]}",
              keyspace);

      String result = RestUtils.post(authToken, basePath, payload, HttpStatus.SC_CREATED);

      assertThat(result).isEqualTo(String.format("{\"name\":\"%s\"}", keyspace));
      // validate with get as well
      String getResult =
          RestUtils.get(authToken, basePath + "/" + keyspace + "?raw=true", HttpStatus.SC_OK);
      JsonNode json = OBJECT_MAPPER.readTree(getResult);
      assertThat(json)
          .isInstanceOfSatisfying(
              ObjectNode.class,
              namespace -> {
                assertThat(namespace.findValue("name").asText()).isEqualTo(keyspace);
                assertThat(namespace.findValue("datacenters"))
                    .isInstanceOfSatisfying(
                        ArrayNode.class,
                        dcs ->
                            assertThat(dcs)
                                .hasSize(1)
                                .anySatisfy(
                                    dc -> {
                                      assertThat(dc.findValuesAsText("name")).containsOnly("dc1");
                                      assertThat(dc.findValue("replicas").asInt()).isEqualTo(1);
                                    }));
              });
    }

    @Test
    public void noPayload() throws IOException {
      String r = RestUtils.post(authToken, basePath, null, 422);

      assertThat(r)
          .isEqualTo("{\"description\":\"Request invalid: payload not provided.\",\"code\":422}");
    }

    @Test
    public void malformedPayload() throws IOException {
      String r = RestUtils.post(authToken, basePath, "{\"malformed\":", 400);

      assertThat(r).isEqualTo("{\"code\":400,\"message\":\"Unable to process JSON\"}");
    }

    @Test
    public void noNamespaceName() throws IOException {
      String r = RestUtils.post(authToken, basePath, "{}", 422);

      assertThat(r)
          .isEqualTo(
              "{\"description\":\"Request invalid: `name` is required to create a namespace.\",\"code\":422}");
    }

    @Test
    public void zeroReplicas() throws IOException {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String payload = String.format("{\"name\": \"%s\", \"replicas\": 0}", keyspace);

      String r = RestUtils.post(authToken, basePath, payload, 422);

      assertThat(r)
          .isEqualTo(
              "{\"description\":\"Request invalid: minimum amount of `replicas` for `SimpleStrategy` is one.\",\"code\":422}");
    }

    @Test
    public void noDatacenterName() throws IOException {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String payload = String.format("{\"name\": \"%s\", \"datacenters\": [ {} ]}", keyspace);

      String r = RestUtils.post(authToken, basePath, payload, 422);

      assertThat(r)
          .isEqualTo(
              "{\"description\":\"Request invalid: a datacenter `name` is required when using `NetworkTopologyStrategy`.\",\"code\":422}");
    }

    @Test
    public void zeroDatacenterReplicas() throws IOException {
      String keyspace = RandomStringUtils.randomAlphanumeric(16);
      String payload =
          String.format(
              "{\"name\": \"%s\", \"datacenters\": [ { \"name\": \"dc1\", \"replicas\": 0 } ]}",
              keyspace);

      String r = RestUtils.post(authToken, basePath, payload, 422);

      assertThat(r)
          .isEqualTo(
              "{\"description\":\"Request invalid: minimum amount of `replicas` for a datacenter is one.\",\"code\":422}");
    }
  }

  @Nested
  @ExtendWith(CqlSessionExtension.class)
  @CqlSessionSpec()
  class GetBuiltInFunctions {
    @Test
    public void happyPath(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String path = basePath + "/" + keyspace.toString() + "/functions";
      String result = RestUtils.get(authToken, path, HttpStatus.SC_OK);
      JsonNode json = OBJECT_MAPPER.readTree(result);
      assertThat(json.hasNonNull("functions")).isTrue();
      assertThat(json.at("/functions").isArray()).isTrue();
      Iterator<JsonNode> it = json.requiredAt("/functions").iterator();
      while (it.hasNext()) {
        JsonNode next = it.next();
        assertThat(next.hasNonNull("name")).isTrue();
        assertThat(next.hasNonNull("description")).isTrue();
      }
    }
  }

  @Nested
  @ExtendWith(CqlSessionExtension.class)
  @CqlSessionSpec()
  class DeleteNamespace {

    @Test
    public void happyPath(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String path = basePath + "/" + keyspace.toString();

      RestUtils.delete(authToken, path, HttpStatus.SC_NO_CONTENT);

      // assert not found anymore
      RestUtils.get(authToken, path, HttpStatus.SC_NOT_FOUND);
    }
  }
}
