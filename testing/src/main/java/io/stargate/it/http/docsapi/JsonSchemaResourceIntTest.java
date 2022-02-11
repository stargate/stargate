package io.stargate.it.http.docsapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.io.Resources;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@NotThreadSafe
public class JsonSchemaResourceIntTest extends BaseIntegrationTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final OkHttpClient CLIENT =
      new OkHttpClient().newBuilder().readTimeout(Duration.ofMinutes(3)).build();

  private static String authToken;
  private static String host;
  private static String hostWithPort;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) throws IOException {
    host = "http://" + cluster.seedAddress();
    hostWithPort = host + ":" + DEFAULT_DOCS_API_PORT;

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

  String getCollectionPath(CqlIdentifier keyspace) {
    return hostWithPort + "/v2/namespaces/" + keyspace + "/collections";
  }

  String getSchemaPath(CqlIdentifier keyspace, String collectionName) {
    return getCollectionPath(keyspace) + "/" + collectionName + "/json-schema";
  }

  @Nested
  @CqlSessionSpec
  @ExtendWith(CqlSessionExtension.class)
  class GetJsonSchema {

    @Test
    public void schemaNotSet(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String collectionName = RandomStringUtils.randomAlphanumeric(10);
      RestUtils.post(
          authToken,
          getCollectionPath(keyspace),
          String.format("{\"name\":\"%s\"}", collectionName),
          201);

      String r = RestUtils.get(authToken, getSchemaPath(keyspace, collectionName), 404);

      assertThat(r)
          .isEqualTo(
              "{\"description\":\"The JSON schema is not set for the collection.\",\"code\":404}");
    }

    @Test
    public void notExistingCollection(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String r = RestUtils.get(authToken, getSchemaPath(keyspace, "not-existing"), 404);

      assertThat(r)
          .isEqualTo("{\"description\":\"Collection not-existing does not exist.\",\"code\":404}");
    }
  }

  @Nested
  @CqlSessionSpec
  @ExtendWith(CqlSessionExtension.class)
  class AttachJsonSchema {

    @Test
    public void happyPath(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String collectionName = RandomStringUtils.randomAlphanumeric(10);
      RestUtils.post(
          authToken,
          getCollectionPath(keyspace),
          String.format("{\"name\":\"%s\"}", collectionName),
          201);

      URL schema = this.getClass().getClassLoader().getResource("schema.json");
      String body = Resources.toString(schema, StandardCharsets.UTF_8);
      RestUtils.put(authToken, getSchemaPath(keyspace, collectionName), body, 200);

      String r = RestUtils.get(authToken, getSchemaPath(keyspace, collectionName), 200);
      assertThat(OBJECT_MAPPER.readTree(r).requiredAt("/schema"))
          .isEqualTo(OBJECT_MAPPER.readTree(body));

      // test valid doc insert and update
      String validDoc = "{\"id\":1, \"name\":\"a\", \"price\":1}";
      RestUtils.post(authToken, getCollectionPath(keyspace) + "/" + collectionName, validDoc, 201);
      RestUtils.put(
          authToken, getCollectionPath(keyspace) + "/" + collectionName + "/1", validDoc, 200);

      // test invalid doc insert and update
      String invalidDoc = "{\"id\":1, \"price\":1}";
      String insertResult =
          RestUtils.post(
              authToken, getCollectionPath(keyspace) + "/" + collectionName, invalidDoc, 400);
      String updateResult =
          RestUtils.put(
              authToken,
              getCollectionPath(keyspace) + "/" + collectionName + "/1",
              invalidDoc,
              400);

      assertThat(insertResult)
          .isEqualTo(updateResult)
          .isEqualTo(
              "{\"description\":\"Invalid JSON: [object has missing required properties ([\\\"name\\\"])]\",\"code\":400}");
    }

    @Test
    public void partialUpdatesNotAllowed(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String collectionName = RandomStringUtils.randomAlphanumeric(10);
      RestUtils.post(
          authToken,
          getCollectionPath(keyspace),
          String.format("{\"name\":\"%s\"}", collectionName),
          201);

      URL schema = this.getClass().getClassLoader().getResource("schema.json");
      String body = Resources.toString(schema, StandardCharsets.UTF_8);
      RestUtils.put(authToken, getSchemaPath(keyspace, collectionName), body, 200);

      String doc = "{\"id\":1, \"name\":\"a\", \"price\":1}";

      // partial updates not allowed
      String updateResult =
          RestUtils.put(
              authToken, getCollectionPath(keyspace) + "/" + collectionName + "/1/path", doc, 400);

      String patchRootResult =
          RestUtils.patch(
              authToken, getCollectionPath(keyspace) + "/" + collectionName + "/1", doc, 400);

      String patchPathResult =
          RestUtils.patch(
              authToken, getCollectionPath(keyspace) + "/" + collectionName + "/1/path", doc, 400);

      assertThat(updateResult)
          .isEqualTo(patchRootResult)
          .isEqualTo(patchPathResult)
          .isEqualTo(
              "{\"description\":\"When a collection has a JSON schema, partial updates of documents are disallowed for performance reasons.\",\"code\":400}");
    }

    @Test
    public void invalidSchema(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String collectionName = RandomStringUtils.randomAlphanumeric(10);
      RestUtils.post(
          authToken,
          getCollectionPath(keyspace),
          String.format("{\"name\":\"%s\"}", collectionName),
          201);

      URL invalidSchema = this.getClass().getClassLoader().getResource("invalid-schema.json");
      String body = Resources.toString(invalidSchema, StandardCharsets.UTF_8);
      String r = RestUtils.put(authToken, getSchemaPath(keyspace, collectionName), body, 400);

      assertThat(r)
          .isEqualTo(
              "{\"description\":\"The provided JSON schema is invalid or malformed.\",\"code\":400}");
    }

    @Test
    public void notExistingCollection(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      URL invalidSchema = this.getClass().getClassLoader().getResource("invalid-schema.json");
      String body = Resources.toString(invalidSchema, StandardCharsets.UTF_8);
      String r = RestUtils.put(authToken, getSchemaPath(keyspace, "not-existing"), body, 404);

      assertThat(r)
          .isEqualTo("{\"description\":\"Collection not-existing does not exist.\",\"code\":404}");
    }
  }
}
