package io.stargate.it.http.docsapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.shaded.guava.common.io.Resources;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@NotThreadSafe
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec()
public abstract class BaseDocumentApiV2Test extends BaseIntegrationTest {
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
    JsonNode obj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));

    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);

    String resp = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    resp = RestUtils.get(authToken, collectionPath + "/1/quiz/maths", 200);
    assertThat(OBJECT_MAPPER.readTree(resp))
        .isEqualTo(wrapResponse(obj.requiredAt("/quiz/maths"), "1", null));

    resp =
        RestUtils.get(
            authToken,
            collectionPath + "?where={\"products.electronics.Pixel_3a.price\":{\"$lt\": 800}}",
            200);
    ObjectNode expected = OBJECT_MAPPER.createObjectNode();
    expected.set("1", obj);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(expected, null, null));
  }

  @Test
  public void testUnauthorized() throws IOException {
    String data =
        OBJECT_MAPPER
            .readTree(this.getClass().getClassLoader().getResource("example.json"))
            .toString();

    // Missing token header
    RestUtils.post(null, collectionPath, data, 401);
    RestUtils.put(null, collectionPath + "/1", data, 401);
    RestUtils.patch(null, collectionPath + "/1", data, 401);
    RestUtils.delete(null, collectionPath + "/1", 401);
    RestUtils.get(null, collectionPath + "/1", 401);
    RestUtils.get(null, collectionPath, 401);

    // Bad token header
    RestUtils.post("garbage", collectionPath, data, 401);
    RestUtils.put("garbage", collectionPath + "/1", data, 401);
    RestUtils.patch("garbage", collectionPath + "/1", data, 401);
    RestUtils.delete("garbage", collectionPath + "/1", 401);
    RestUtils.get("garbage", collectionPath + "/1", 401);
    RestUtils.get("garbage", collectionPath, 401);
  }

  @Test
  public void testMalformedBadRequest() throws IOException {
    String malformedJson = "{\"malformed\": ";
    RestUtils.post(authToken, collectionPath, malformedJson, 400);
    RestUtils.post(authToken, collectionPath + "/batch", "[" + malformedJson + "]", 400);
    RestUtils.put(authToken, collectionPath + "/1", malformedJson, 400);
    RestUtils.patch(authToken, collectionPath + "/1", malformedJson, 400);
    RestUtils.get(authToken, collectionPath + "/1?where=" + malformedJson, 400);
    RestUtils.get(
        authToken,
        collectionPath
            + "/1?where={\"a\":{\"$eq\":\"b\"}}&fields=["
            + malformedJson
            + "]"
            + malformedJson,
        400);
    RestUtils.get(authToken, collectionPath + "?where=" + malformedJson, 400);
    RestUtils.get(
        authToken,
        collectionPath
            + "?where={\"a\":{\"$eq\":\"b\"}}&fields=["
            + malformedJson
            + "]"
            + malformedJson,
        400);
  }

  @Test
  public void testInvalidKeyspaceAndTable() throws IOException {
    JsonNode obj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String data = obj.toString();
    String resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/unknown_keyspace_1337/collections/collection/1",
            data,
            404);
    assertThat(resp)
        .isEqualTo(
            "{\"description\":\"Unknown namespace unknown_keyspace_1337, you must create it first.\",\"code\":404}");

    resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/invalid-character/1",
            data,
            400);
    assertThat(resp)
        .isEqualTo(
            "{\"description\":\"Could not create collection invalid-character, it has invalid characters. Valid characters are alphanumeric and underscores.\",\"code\":400}");
  }

  @Test
  public void testAccessArbitraryTableDisallowed(CqlSession session) throws IOException {
    assertThat(
            session
                .execute(
                    String.format(
                        "create table \"%s\".not_docs(x text primary key, y text)", keyspace))
                .wasApplied())
        .isTrue();

    String errorMessage =
        String.format(
            "{\"description\":\"The database table %s.not_docs is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted.\",\"code\":400}",
            keyspace);

    String resp =
        RestUtils.post(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/not_docs",
            "{\"a\": \"b\"}",
            400);
    assertThat(resp).isEqualTo(errorMessage);

    resp =
        RestUtils.patch(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/not_docs/1",
            "{\"a\": \"b\"}",
            400);
    assertThat(resp).isEqualTo(errorMessage);

    resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/not_docs/1",
            "{\"a\": \"b\"}",
            400);
    assertThat(resp).isEqualTo(errorMessage);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/not_docs/1",
            400);
    assertThat(resp).isEqualTo(errorMessage);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/not_docs/1?where={\"a\":{\"$eq\":1}}",
            400);
    assertThat(resp).isEqualTo(errorMessage);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/not_docs?where={\"a\":{\"$eq\":1}}",
            400);
    assertThat(resp).isEqualTo(errorMessage);
  }

  @Test
  public void testDocGetOnNotExistingNamespace() throws IOException {
    String resp =
        RestUtils.get(
            authToken, hostWithPort + "/v2/namespaces/unknown/collections/collection/1", 404);
    assertThat(resp)
        .isEqualTo("{\"description\":\"Namespace unknown does not exist.\",\"code\":404}");
  }

  @Test
  public void testDocGetOnNotExistingCollection() throws IOException {
    String resp =
        RestUtils.get(
            authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/unknown/1", 404);
    assertThat(resp)
        .isEqualTo("{\"description\":\"Collection unknown does not exist.\",\"code\":404}");
  }

  @Test
  public void testInvalidKeyPut() throws IOException {
    JsonNode obj = OBJECT_MAPPER.readTree("{ \"bracketedarraypaths[100]\": \"are not allowed\" }");

    String resp = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 400);
    assertThat(resp)
        .isEqualTo(
            "{\"description\":\"Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field bracketedarraypaths[100]\",\"code\":400}");

    obj = OBJECT_MAPPER.readTree("{ \"periods.something\": \"are not allowed\" }");

    resp = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 400);
    assertThat(resp)
        .isEqualTo(
            "{\"description\":\"Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field periods.something\",\"code\":400}");

    obj = OBJECT_MAPPER.readTree("{ \"single'quotes\": \"are not allowed\" }");

    resp = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 400);
    assertThat(resp)
        .isEqualTo(
            "{\"description\":\"Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field single'quotes\",\"code\":400}");

    obj = OBJECT_MAPPER.readTree("{ \"back\\\\\\\\slashes\": \"are not allowed\" }");
    resp = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 400);
    assertThat(resp)
        .isEqualTo(
            "{\"description\":\"Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field back\\\\\\\\slashes\",\"code\":400}");
  }

  @Test
  public void testEscapableKeyPut() throws IOException {
    JsonNode obj = OBJECT_MAPPER.readTree("{ \"periods\\\\.\": \"are allowed if escaped\" }");
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);
    String resp = RestUtils.get(authToken, collectionPath + "/1?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(resp))
        .isEqualTo(OBJECT_MAPPER.readTree("{\"periods.\": \"are allowed if escaped\" }"));

    obj = OBJECT_MAPPER.readTree("{ \"*aste*risks*\": \"are allowed\" }");
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);
    resp = RestUtils.get(authToken, collectionPath + "/1?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(obj);

    resp = RestUtils.put(authToken, collectionPath + "/1", "", 422);
    assertThat(resp)
        .isEqualTo(
            "{\"description\":\"Request invalid: payload must not be empty.\",\"code\":422}");
  }

  @Test
  public void testTtlPut() throws IOException {
    JsonNode obj1 = OBJECT_MAPPER.readTree("{ \"delete this\": \"in ten seconds\" }");
    JsonNode obj2 = OBJECT_MAPPER.readTree("{ \"do not delete\": \"this\" }");
    RestUtils.put(authToken, collectionPath + "/1?ttl=1", obj1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/2", obj2.toString(), 200);

    Awaitility.await()
        .atLeast(0, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              // After the TTL is up, obj1 should be gone and obj2 should have no key 'a'
              RestUtils.get(authToken, collectionPath + "/1?raw=true", 404);
              String resp = RestUtils.get(authToken, collectionPath + "/2?raw=true", 200);
              assertThat(OBJECT_MAPPER.readTree(resp))
                  .isEqualTo(OBJECT_MAPPER.readTree("{ \"do not delete\": \"this\" }"));
            });
  }

  @Test
  public void testInvalidTtl() throws IOException {
    JsonNode obj1 = OBJECT_MAPPER.readTree("{ \"delete this\": \"in 5 seconds\" }");
    JsonNode obj2 = OBJECT_MAPPER.readTree("{ \"match the parent\": \"this\", \"a\": \"b\" }");
    RestUtils.put(authToken, collectionPath + "/1?ttl=-1", obj1.toString(), 400);
    RestUtils.put(authToken, collectionPath + "/1?ttl=auto", obj2.toString(), 404);
  }

  @Test
  public void testPatchWithAutoTtl() throws IOException {
    JsonNode obj1 = OBJECT_MAPPER.readTree("{ \"delete this\": \"in 5 seconds\" }");
    JsonNode obj2 = OBJECT_MAPPER.readTree("{ \"match the parent\": \"this\", \"a\": \"b\" }");
    RestUtils.put(authToken, collectionPath + "/1?ttl=5", obj1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/1/b?ttl-auto=true", obj2.toString(), 200);

    Awaitility.await()
        .atLeast(4000, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              // After the TTL is up, obj1 should be gone, with no remnants
              RestUtils.get(authToken, collectionPath + "/1?raw=true", 404);
            });
  }

  @Test
  public void testPatchWithAutoTtlNullParent() throws IOException, InterruptedException {
    JsonNode obj1 = OBJECT_MAPPER.readTree("{ \"delete this\": \"in 5 seconds\" }");
    JsonNode obj2 = OBJECT_MAPPER.readTree("{ \"match the parent\": \"this\", \"a\": \"b\" }");
    // No ttl on the parent
    RestUtils.put(authToken, collectionPath + "/1", obj1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/1/b?ttl=auto", obj2.toString(), 200);

    TimeUnit.SECONDS.sleep(5);

    String res = RestUtils.get(authToken, collectionPath + "/1/b?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(res)).isEqualTo(obj2);
  }

  @Test
  public void testWeirdButAllowedKeys() throws IOException {
    JsonNode obj = OBJECT_MAPPER.readTree("{ \"$\": \"weird but allowed\" }");
    RestUtils.put(authToken, collectionPath + "/1/path", obj.toString(), 200);

    String resp = RestUtils.get(authToken, collectionPath + "/1/path", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("{ \"$30\": \"not as weird\" }");
    RestUtils.put(authToken, collectionPath + "/1/path", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/1/path", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("{ \"@\": \"weird but allowed\" }");
    RestUtils.put(authToken, collectionPath + "/1/path", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/1/path", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("{ \"meet me @ the place\": \"not as weird\" }");
    RestUtils.put(authToken, collectionPath + "/1/path", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/1/path", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("{ \"?\": \"weird but allowed\" }");
    RestUtils.put(authToken, collectionPath + "/1/path", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/1/path", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("{ \"spac es\": \"weird but allowed\" }");
    RestUtils.put(authToken, collectionPath + "/1/path", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/1/path", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("{ \"3\": [\"totally allowed\"] }");
    RestUtils.put(authToken, collectionPath + "/1/path", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/1/path", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    resp = RestUtils.get(authToken, collectionPath + "/1/path/3/[0]", 200);
    assertThat(OBJECT_MAPPER.readTree(resp))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree("\"totally allowed\""), "1", null));

    obj = OBJECT_MAPPER.readTree("{ \"-1\": \"totally allowed\" }");
    RestUtils.put(authToken, collectionPath + "/1/path", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/1/path", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    resp = RestUtils.get(authToken, collectionPath + "/1/path/-1", 200);
    assertThat(OBJECT_MAPPER.readTree(resp))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree("\"totally allowed\""), "1", null));

    obj = OBJECT_MAPPER.readTree("{ \"Eric says \\\"hello\\\"\": \"totally allowed\" }");
    RestUtils.put(authToken, collectionPath + "/1/path", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/1/path", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));
  }

  @Test
  public void testInvalidDepthAndLength() throws IOException {
    JsonNode obj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("tooDeep.json"));
    String resp = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 400);
    assertThat(resp).isEqualTo("{\"description\":\"Max depth of 64 exceeded.\",\"code\":400}");

    obj = OBJECT_MAPPER.readTree("{ \"some\": \"json\" }");
    resp = RestUtils.put(authToken, collectionPath + "/1/[1000000]", obj.toString(), 400);
    assertThat(resp)
        .isEqualTo("{\"description\":\"Max array length of 1000000 exceeded.\",\"code\":400}");
  }

  @Test
  public void testArrayGet() throws IOException {
    JsonNode obj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String resp = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);
    assertThat(resp).isEqualTo("{\"documentId\":\"1\"}");

    resp = RestUtils.get(authToken, collectionPath + "/1/quiz/maths/q1/options/[0]", 200);
    assertThat(OBJECT_MAPPER.readTree(resp))
        .isEqualTo(wrapResponse(obj.requiredAt("/quiz/maths/q1/options/0"), "1", null));

    resp = RestUtils.get(authToken, collectionPath + "/1/quiz/maths/q1/options/[0]?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(obj.requiredAt("/quiz/maths/q1/options/0"));

    resp = RestUtils.get(authToken, collectionPath + "/1/quiz/nests/q1/options/[3]/this", 200);
    assertThat(OBJECT_MAPPER.readTree(resp))
        .isEqualTo(wrapResponse(obj.requiredAt("/quiz/nests/q1/options/3/this"), "1", null));
  }

  @Test
  public void testGetProfile() throws IOException {
    JsonNode obj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);

    String resp = RestUtils.get(authToken, collectionPath + "/1?profile=true", 200);
    assertThat(OBJECT_MAPPER.readTree(resp).get("profile").isEmpty()).isFalse();
  }

  @Test
  public void testInvalidPathGet() throws IOException {
    JsonNode obj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String resp = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);
    assertThat(resp).isEqualTo("{\"documentId\":\"1\"}");

    RestUtils.get(authToken, collectionPath + "/1/nonexistent/path", 404);

    RestUtils.get(authToken, collectionPath + "/1/nonexistent/path/[1]", 404);

    RestUtils.get(
        authToken, collectionPath + "/1/quiz/maths/q1/options/[9999]", 404); // out of bounds
  }

  @Test
  public void testEscapedCharGet() throws IOException {
    JsonNode obj =
        OBJECT_MAPPER.readTree(
            "{\"a\\\\.b\":\"somedata\",\"some,data\":\"something\",\"*\":\"star\"}");
    String resp = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);
    assertThat(resp).isEqualTo("{\"documentId\":\"1\"}");

    String result = RestUtils.get(authToken, collectionPath + "/1/a%5C.b?raw=true", 200);
    assertThat(result).isEqualTo("\"somedata\"");

    result = RestUtils.get(authToken, collectionPath + "/1/some%5C,data?raw=true", 200);
    assertThat(result).isEqualTo("\"something\"");

    result = RestUtils.get(authToken, collectionPath + "/1/%5C*?raw=true", 200);
    assertThat(result).isEqualTo("\"star\"");
  }

  @Test
  public void testPutNullsAndEmpties() throws IOException {
    JsonNode obj = OBJECT_MAPPER.readTree("{\"abc\": null}");
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);

    String resp = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("{\"abc\": {}}");
    RestUtils.put(authToken, collectionPath + "/2", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/2", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "2", null));

    obj = OBJECT_MAPPER.readTree("{\"abc\": []}");
    RestUtils.put(authToken, collectionPath + "/3", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/3", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "3", null));

    obj =
        OBJECT_MAPPER.readTree(
            "{\"abc\": [], \"bcd\": {}, \"cde\": null, \"abcd\": { \"nest1\": [], \"nest2\": {}}}");
    RestUtils.put(authToken, collectionPath + "/4", obj.toString(), 200);

    resp = RestUtils.get(authToken, collectionPath + "/4", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "4", null));

    resp = RestUtils.get(authToken, collectionPath + "/4/abc", 200);
    assertThat(OBJECT_MAPPER.readTree(resp))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.createArrayNode(), "4", null));

    resp = RestUtils.get(authToken, collectionPath + "/4/bcd", 200);
    assertThat(OBJECT_MAPPER.readTree(resp))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.createObjectNode(), "4", null));

    resp = RestUtils.get(authToken, collectionPath + "/4/abcd/nest1", 200);
    assertThat(OBJECT_MAPPER.readTree(resp))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.createArrayNode(), "4", null));
  }

  @Test
  public void testPutSimpleArray() throws IOException {
    JsonNode obj = OBJECT_MAPPER.readTree("[1, 2, 3, 4, 5]");
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);

    String resp = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));
  }

  @Test
  public void testPutReplacingObject() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r = RestUtils.put(authToken, collectionPath + "/1", fullObj.toString(), 200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    JsonNode obj;
    obj = OBJECT_MAPPER.readTree("{\"q5000\": \"hello?\"}");
    RestUtils.put(authToken, collectionPath + "/1/quiz/sport", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1/quiz/sport", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    r = RestUtils.get(authToken, collectionPath + "/1", 200);

    ObjectNode sportNode = OBJECT_MAPPER.createObjectNode();
    sportNode.set("q5000", TextNode.valueOf("hello?"));

    ObjectNode fullObjNode = (ObjectNode) fullObj;
    ((ObjectNode) fullObjNode.get("quiz")).set("sport", sportNode);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(fullObjNode, "1", null));
  }

  @Test
  public void testPutReplacingArrayElement() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r = RestUtils.put(authToken, collectionPath + "/1", fullObj.toString(), 200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    JsonNode obj;
    obj = OBJECT_MAPPER.readTree("{\"q5000\": \"hello?\"}");
    RestUtils.put(authToken, collectionPath + "/1/quiz/nests/q1/options/[0]", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1/quiz/nests/q1/options/[0]", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    r = RestUtils.get(authToken, collectionPath + "/1", 200);

    ObjectNode optionNode = OBJECT_MAPPER.createObjectNode();
    optionNode.set("q5000", TextNode.valueOf("hello?"));

    ObjectNode fullObjNode = (ObjectNode) fullObj;
    ((ArrayNode) fullObjNode.at("/quiz/nests/q1/options")).set(0, optionNode);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(fullObjNode, "1", null));
  }

  @Test
  public void testPutReplacingWithArray() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r = RestUtils.put(authToken, collectionPath + "/1", fullObj.toString(), 200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    JsonNode obj = OBJECT_MAPPER.readTree("[{\"array\": \"at\"}, \"sub\", \"doc\"]");
    RestUtils.put(authToken, collectionPath + "/1/quiz", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1/quiz", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("[0, \"a\", \"2\", true]");
    RestUtils.put(authToken, collectionPath + "/1/quiz/nests/q1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1/quiz/nests/q1", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    ObjectNode nestsNode =
        (ObjectNode) OBJECT_MAPPER.readTree("{\"nests\":{\"q1\":[0,\"a\",\"2\",true]}}");
    ObjectNode fullObjNode = (ObjectNode) fullObj;
    fullObjNode.set("quiz", nestsNode);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(fullObjNode, "1", null));

    obj = OBJECT_MAPPER.readTree("[{\"array\": \"at\"}, \"\", \"doc\"]");
    RestUtils.put(authToken, collectionPath + "/1/quiz", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1/quiz", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("{\"we\": {\"are\": \"done\"}}");
    RestUtils.put(authToken, collectionPath + "/1/quiz", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1/quiz", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));
  }

  @Test
  public void testPutProfile() throws IOException {
    JsonNode obj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String resp = RestUtils.put(authToken, collectionPath + "/1?profile=true", obj.toString(), 200);
    assertThat(OBJECT_MAPPER.readTree(resp).get("profile").isEmpty()).isFalse();
  }

  @Test
  public void testPutDocPathProfile() throws IOException {
    JsonNode obj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);

    obj = OBJECT_MAPPER.readTree("[0, \"a\", \"2\", true]");
    String resp =
        RestUtils.put(
            authToken, collectionPath + "/1/quiz/nests/q1?profile=true", obj.toString(), 200);
    assertThat(OBJECT_MAPPER.readTree(resp).get("profile").isEmpty()).isFalse();
  }

  @Test
  public void testPutOverwriteWithPrimitive() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/1", fullObj.toString(), 200);

    JsonNode obj = OBJECT_MAPPER.readTree("true");
    RestUtils.put(authToken, collectionPath + "/1/quiz", obj.toString(), 200);

    String r = RestUtils.get(authToken, collectionPath + "/1?raw=true", 200);
    JsonNode resultNode = OBJECT_MAPPER.readTree(r);
    assertThat(resultNode.requiredAt("/quiz").booleanValue()).isEqualTo(true);
  }

  @Test
  public void testPutOverwriteWithNull() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/1", fullObj.toString(), 200);

    JsonNode obj = OBJECT_MAPPER.readTree("null");
    RestUtils.put(authToken, collectionPath + "/1/quiz", obj.toString(), 200);

    String r = RestUtils.get(authToken, collectionPath + "/1?raw=true", 200);
    JsonNode resultNode = OBJECT_MAPPER.readTree(r);
    assertThat(resultNode.requiredAt("/quiz").isNull()).isEqualTo(true);
  }

  @Test
  public void testPutOverwriteWithPrimitiveBackAndForth() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/1", fullObj.toString(), 200);

    JsonNode primitive = OBJECT_MAPPER.readTree("true");
    RestUtils.put(authToken, collectionPath + "/1/quiz", primitive.toString(), 200);

    JsonNode obj = OBJECT_MAPPER.readTree("{\"some\": \"value\"}");
    RestUtils.put(authToken, collectionPath + "/1/quiz", obj.toString(), 200);

    String r = RestUtils.get(authToken, collectionPath + "/1?raw=true", 200);
    JsonNode resultNode = OBJECT_MAPPER.readTree(r);
    assertThat(resultNode.requiredAt("/quiz")).isEqualTo(obj);
  }

  @Test
  public void testInvalidPuts() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r = RestUtils.put(authToken, collectionPath + "/1", fullObj.toString(), 200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    JsonNode obj;
    obj = OBJECT_MAPPER.readTree("3");
    r = RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Updating a key with just a JSON primitive is not allowed. Hint: update the parent path with a defined object instead.\",\"code\":400}");

    r = RestUtils.put(authToken, collectionPath + "/1/quiz/sport", "", 422);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Request invalid: payload must not be empty.\",\"code\":422}");
  }

  @Test
  public void testDelete() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r = RestUtils.put(authToken, collectionPath + "/1", fullObj.toString(), 200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    RestUtils.delete(authToken, collectionPath + "/1/quiz/sport/q1/question", 204);

    RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/question", 404);

    RestUtils.delete(authToken, collectionPath + "/1/quiz/maths", 204);

    RestUtils.get(authToken, collectionPath + "/1/quiz/maths", 404);

    RestUtils.delete(authToken, collectionPath + "/1/quiz/nests/q1/options/[0]", 204);

    r = RestUtils.get(authToken, collectionPath + "/1/quiz/nests/q1/options", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree(
                    "[null,\"not a nest\",\"definitely not a nest\",{ \"this\":  true }]"),
                "1",
                null));

    RestUtils.delete(authToken, collectionPath + "/1", 204);

    RestUtils.get(authToken, collectionPath + "/1", 404);
  }

  @Test
  public void testPost() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response resp = RestUtils.postRaw(authToken, collectionPath, fullObj.toString(), 201);
    String newLocation = resp.header("location");
    String body = resp.body().string();
    String newId = OBJECT_MAPPER.readTree(body).requiredAt("/documentId").asText();
    assertThat(newId).isNotNull();

    String r = RestUtils.get(authToken, newLocation, 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(fullObj, newId, null));
  }

  @Test
  public void testPostProfile() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String resp =
        RestUtils.post(authToken, collectionPath + "?profile=true", fullObj.toString(), 201);
    assertThat(OBJECT_MAPPER.readTree(resp).get("profile").isEmpty()).isFalse();
  }

  @Test
  public void testJsonEmptyKey() throws IOException {
    String json = "{\"\": \"value\", \"nested\": {\"\": \"value\"}}";

    String resp = RestUtils.post(authToken, collectionPath, json, 400);

    assertThat(resp)
        .isEqualTo(
            "{\"description\":\"JSON objects containing empty field names are not supported at the moment.\",\"code\":400}");
  }

  @Test
  public void testWriteManyDocs() throws IOException {
    // Create documents using multiExample that creates random ID's
    URL url = Resources.getResource("multiExample.json");
    String body = Resources.toString(url, StandardCharsets.UTF_8);
    String resp = RestUtils.post(authToken, collectionPath + "/batch", body, 202);
    JsonNode respBody = OBJECT_MAPPER.readTree(resp);
    ArrayNode documentIds = (ArrayNode) respBody.requiredAt("/documentIds");
    assertThat(documentIds.size()).isEqualTo(27);
    Iterator<JsonNode> iter = documentIds.iterator();
    while (iter.hasNext()) {
      String docId = iter.next().textValue();
      resp = RestUtils.get(authToken, collectionPath + "/" + docId, 200);
      respBody = OBJECT_MAPPER.readTree(resp);
      assertThat(respBody.at("/id")).isNotNull();
      assertThat(respBody.at("/b")).isNotNull();
    }
  }

  @Test
  public void testWriteManyDocsWithTtl() throws IOException {
    // Create documents using multiExample that creates random ID's
    URL url = Resources.getResource("multiExample.json");
    String body = Resources.toString(url, StandardCharsets.UTF_8);
    String resp = RestUtils.post(authToken, collectionPath + "/batch?ttl=1", body, 202);
    JsonNode respBody = OBJECT_MAPPER.readTree(resp);
    ArrayNode documentIds = (ArrayNode) respBody.requiredAt("/documentIds");
    assertThat(documentIds.size()).isEqualTo(27);

    Awaitility.await()
        .atLeast(0, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              Iterator<JsonNode> iter = documentIds.iterator();
              // After the TTL is up, all the docs should be gone
              while (iter.hasNext()) {
                String docId = iter.next().textValue();
                RestUtils.get(authToken, collectionPath + "/" + docId, 404);
              }
            });
  }

  @Test
  public void testWriteManyDocsWithIdPath() throws IOException {
    // Create documents using multiExample that sets the document ID's using a particular path in
    // the document.
    URL url = Resources.getResource("multiExample.json");
    String body = Resources.toString(url, StandardCharsets.UTF_8);
    String resp = RestUtils.post(authToken, collectionPath + "/batch?id-path=id.[0]", body, 202);
    JsonNode respBody = OBJECT_MAPPER.readTree(resp);
    ArrayNode documentIds = (ArrayNode) respBody.requiredAt("/documentIds");
    assertThat(documentIds.size()).isEqualTo(27);
    Iterator<JsonNode> iter = documentIds.iterator();
    List<String> expectedIds =
        Arrays.asList(
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
            "r", "s", "t", "u", "v", "w", "x", "y", "z", "aa");
    int i = 0;
    while (iter.hasNext()) {
      String docId = iter.next().textValue();
      assertThat(docId).isEqualTo(expectedIds.get(i++));
      resp = RestUtils.get(authToken, collectionPath + "/" + docId, 200);
      respBody = OBJECT_MAPPER.readTree(resp);
      assertThat(respBody.at("/id")).isNotNull();
      assertThat(respBody.at("/b")).isNotNull();
    }
  }

  @Test
  public void testWriteManyDocsOverwrite() throws IOException {
    // Create documents using multiExample that sets the document ID's using a particular path in
    // the document, overwriting pre-existing data.
    RestUtils.put(authToken, collectionPath + "/aa", "{\"start\":\"value\"}", 200);
    URL url = Resources.getResource("multiExample.json");
    String body = Resources.toString(url, StandardCharsets.UTF_8);
    String resp = RestUtils.post(authToken, collectionPath + "/batch?id-path=id.[0]", body, 202);
    JsonNode respBody = OBJECT_MAPPER.readTree(resp);
    ArrayNode documentIds = (ArrayNode) respBody.requiredAt("/documentIds");
    assertThat(documentIds.size()).isEqualTo(27);

    resp = RestUtils.get(authToken, collectionPath + "/aa?raw=true", 200);
    respBody = OBJECT_MAPPER.readTree(resp);
    assertThat(respBody).isEqualTo(OBJECT_MAPPER.readTree("{\"id\":[\"aa\"], \"b\":\"c\"}"));
  }

  @Test
  public void testWriteManyDocsDuplicateId() throws IOException {
    String body = "[{\"id\":\"1\"},{\"id\":\"1\"}]";

    String resp = RestUtils.post(authToken, collectionPath + "/batch?id-path=id", body, 400);

    JsonNode respBody = OBJECT_MAPPER.readTree(resp);
    assertThat(respBody.requiredAt("/description").asText())
        .isEqualTo(
            "Found duplicate ID 1 in more than one document when doing batched document write.");
  }

  @Test
  public void testWriteManyDocsInvalidPath() throws IOException {
    URL url = Resources.getResource("multiExample.json");
    String body = Resources.toString(url, StandardCharsets.UTF_8);
    String resp =
        RestUtils.post(authToken, collectionPath + "/batch?id-path=no.good.path", body, 400);
    JsonNode respBody = OBJECT_MAPPER.readTree(resp);
    assertThat(respBody.requiredAt("/description").asText())
        .isEqualTo(
            "JSON document {\"id\":[\"a\"],\"a\":\"b\"} requires a String value at the path /no/good/path in order to resolve document ID, found missing node. Batch write failed.");
  }

  @Test
  public void testRootDocumentPatch() throws IOException {
    JsonNode obj = OBJECT_MAPPER.readTree("{\"abc\": 1}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    String r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": true}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(OBJECT_MAPPER.readTree("{ \"abc\": 1, \"bcd\": true }"), "1", null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": {\"a\": {\"b\": 0 }}}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree("{ \"abc\": 1, \"bcd\": {\"a\": {\"b\": 0 }} }"),
                "1",
                null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": [1,2,3,4]}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(OBJECT_MAPPER.readTree("{ \"abc\": 1, \"bcd\": [1,2,3,4] }"), "1", null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": [5,{\"a\": 23},7,8]}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree("{ \"abc\": 1, \"bcd\": [5,{\"a\": 23},7,8] }"), "1", null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree(
                    "{ \"abc\": 1, \"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] }"),
                "1",
                null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": {\"replace\": \"array\"}}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree("{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"} }"),
                "1",
                null));

    obj = OBJECT_MAPPER.readTree("{\"done\": \"done\"}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree(
                    "{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"}, \"done\": \"done\" }"),
                "1",
                null));
  }

  @Test
  public void testRootDocumentPatchNulls() throws IOException {
    JsonNode obj = OBJECT_MAPPER.readTree("{\"abc\": null}");
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);

    String r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": null}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(OBJECT_MAPPER.readTree("{ \"abc\": null, \"bcd\": null }"), "1", null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": {\"a\": {\"b\": null }}}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree("{ \"abc\": null, \"bcd\": {\"a\": {\"b\": null }} }"),
                "1",
                null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": [null,2,null,4]}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree("{ \"abc\": null, \"bcd\": [null,2,null,4] }"), "1", null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": [1,{\"a\": null},3,4]}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree("{ \"abc\": null, \"bcd\": [1,{\"a\": null},3,4] }"),
                "1",
                null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": [null]}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(OBJECT_MAPPER.readTree("{ \"abc\": null, \"bcd\": [null] }"), "1", null));

    obj = OBJECT_MAPPER.readTree("{\"null\": null}");
    RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree("{ \"abc\": null, \"bcd\": [null], \"null\": null }"),
                "1",
                null));
  }

  @Test
  public void testSubDocumentPatch() throws IOException {
    JsonNode obj = OBJECT_MAPPER.readTree("{\"abc\": null}");
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);

    obj = OBJECT_MAPPER.readTree("{\"bcd\": {}}}");
    RestUtils.patch(authToken, collectionPath + "/1/abc", obj.toString(), 200);

    String r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(OBJECT_MAPPER.readTree("{ \"abc\": { \"bcd\": {} } } }"), "1", null));

    obj = OBJECT_MAPPER.readTree("3");
    RestUtils.patch(authToken, collectionPath + "/1/abc/bcd", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(OBJECT_MAPPER.readTree("{ \"abc\": { \"bcd\": 3 } } }"), "1", null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": [null,2,null,4]}");
    RestUtils.patch(authToken, collectionPath + "/1/abc", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree("{ \"abc\": {\"bcd\": [null,2,null,4]} }"), "1", null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": [1,{\"a\": null},3,4]}");
    RestUtils.patch(authToken, collectionPath + "/1/abc", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree("{ \"abc\": { \"bcd\": [1,{\"a\": null},3,4] }}"),
                "1",
                null));

    obj = OBJECT_MAPPER.readTree("{\"bcd\": [null]}");
    RestUtils.patch(authToken, collectionPath + "/1/abc", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(OBJECT_MAPPER.readTree("{ \"abc\": { \"bcd\": [null] } }"), "1", null));

    obj = OBJECT_MAPPER.readTree("{\"null\": null}");
    RestUtils.patch(authToken, collectionPath + "/1/abc", obj.toString(), 200);

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(
            wrapResponse(
                OBJECT_MAPPER.readTree("{ \"abc\": { \"bcd\": [null], \"null\": null }}"),
                "1",
                null));
  }

  @Test
  public void testInvalidPatches() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r = RestUtils.put(authToken, collectionPath + "/1", fullObj.toString(), 200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r = RestUtils.get(authToken, collectionPath + "/1", 200);
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    JsonNode obj;
    obj = OBJECT_MAPPER.readTree("[{\"array\": \"at\"}, \"root\", \"doc\"]");
    r = RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"A patch operation must be done with a JSON object, not an array.\",\"code\":400}");

    // For patching, you must use an object, so arrays even patched to sub-paths are not allowed.
    r = RestUtils.patch(authToken, collectionPath + "/1/quiz/sport/q1", obj.toString(), 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"A patch operation must be done with a JSON object, not an array.\",\"code\":400}");

    obj = OBJECT_MAPPER.readTree("3");
    r = RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Updating a key with just a JSON primitive is not allowed. Hint: update the parent path with a defined object instead.\",\"code\":400}");

    obj = OBJECT_MAPPER.readTree("null");
    r = RestUtils.patch(authToken, collectionPath + "/1", obj.toString(), 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Updating a key with just a JSON primitive is not allowed. Hint: update the parent path with a defined object instead.\",\"code\":400}");

    obj = OBJECT_MAPPER.readTree("{}");
    r = RestUtils.patch(authToken, collectionPath + "/1/quiz/sport", obj.toString(), 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"A patch operation must be done with a non-empty JSON object.\",\"code\":400}");

    r = RestUtils.patch(authToken, collectionPath + "/1/quiz/sport", "", 422);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Request invalid: payload must not be empty.\",\"code\":422}");
  }

  @Test
  public void testDocumentPatchProfile() throws IOException {
    JsonNode obj = OBJECT_MAPPER.readTree("{\"abc\": null}");
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);

    obj = OBJECT_MAPPER.readTree("{\"bcd\": null}");
    String r = RestUtils.patch(authToken, collectionPath + "/1?profile=true", obj.toString(), 200);
    assertThat(OBJECT_MAPPER.readTree(r).get("profile").isEmpty()).isFalse();
  }

  @Test
  public void testDocumentPathPatchProfile() throws IOException {
    JsonNode obj = OBJECT_MAPPER.readTree("{\"abc\": \"string1\"}");
    RestUtils.put(authToken, collectionPath + "/1", obj.toString(), 200);

    obj = OBJECT_MAPPER.readTree("{\"bcd\": null}");
    String r =
        RestUtils.patch(authToken, collectionPath + "/1/abc?profile=true", obj.toString(), 200);
    assertThat(OBJECT_MAPPER.readTree(r).get("profile").isEmpty()).isFalse();
  }

  @Test
  public void testBasicSearch() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // EQ
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.Pixel_3a.price\": {\"$eq\": 600}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    RestUtils.get(
        authToken,
        collectionPath + "/cool-search-id?where={\"price\": {\"$eq\": 600}}&raw=true",
        204);

    // LT
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.food.*.price\": {\"$lt\": 600}}&raw=true",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    // LTE
    r =
        RestUtils.get(
            authToken,
            collectionPath + "/cool-search-id?where={\"products.food.*.price\": {\"$lte\": 600}}",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // GT
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.*.price\": {\"$gt\": 600}}&raw=true",
            200);

    searchResultStr = "[{\"products\": {\"electronics\": {\"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    // GTE
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.*.price\": {\"$gte\": 600}}&raw=true",
            200);

    searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    // EXISTS
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.*.*.price\": {\"$exists\": true}}&raw=true",
            200);
    searchResultStr =
        "["
            + "{\"products\":{\"electronics\":{\"Pixel_3a\":{\"price\":600}}}},"
            + "{\"products\":{\"electronics\":{\"iPhone_11\":{\"price\":900}}}},"
            + "{\"products\":{\"food\":{\"Apple\":{\"price\":0.99}}}},"
            + "{\"products\":{\"food\":{\"Pear\":{\"price\":0.89}}}}"
            + "]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));
  }

  @Test
  public void testBasicSearchWithNegation() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // NOT NE === EQ
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"$not\": {\"products.electronics.Pixel_3a.price\": {\"$ne\": 600}}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testBasicSearchWithSelectivityHints() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={"
                + "\"products.electronics.Pixel_3a.price\": {\"$gte\": 600},"
                + "\"products.electronics.Pixel_3a.price\": {\"$lte\": 600, \"$selectivity\": 0.5}"
                + "}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testBasicSearchEscaped() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(
            "{\"a\\\\.b\":\"somedata\",\"some,data\":\"something\",\"*\":\"star\"}");
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // With escaped period
    String r =
        RestUtils.get(
            authToken,
            collectionPath + "/cool-search-id?where={\"a\\\\.b\": {\"$eq\": \"somedata\"}}",
            200);

    String searchResultStr = "[{\"a.b\":\"somedata\"}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    RestUtils.get(
        authToken,
        collectionPath + "/cool-search-id?where={\"a.b\": {\"$eq\": \"somedata\"}}&raw=true",
        204);

    // With commas
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"some\\\\,data\": {\"$eq\": \"something\"}}&raw=true",
            200);

    searchResultStr = "[{\"some,data\":\"something\"}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    // With asterisk
    r =
        RestUtils.get(
            authToken,
            collectionPath + "/cool-search-id?where={\"\\\\*\": {\"$eq\": \"star\"}}&raw=true",
            200);

    searchResultStr = "[{\"*\":\"star\"}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));
  }

  @Test
  public void testBasicSearchSelectionSet() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // EQ
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.Pixel_3a.price\": {\"$eq\": 600}}&fields=[\"name\", \"price\", \"model\", \"manufacturer\"]",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"name\": \"Pixel\", \"manufacturer\": \"Google\", \"model\": \"3a\", \"price\": 600}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // LT
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.food.*.price\": {\"$lt\": 600}}&fields=[\"name\", \"price\", \"model\"]&raw=true",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"name\": \"apple\", \"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"name\": \"pear\", \"price\": 0.89}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    // LTE
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.food.*.price\": {\"$lte\": 600}}&fields=[\"price\", \"sku\"]",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99, \"sku\": \"100100010101001\"}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89, \"sku\": null}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // GT
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.*.price\": {\"$gt\": 600}}&fields=[\"price\", \"throwaway\"]&raw=true",
            200);

    searchResultStr = "[{\"products\": {\"electronics\": {\"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    // GTE
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.*.price\": {\"$gte\": 600}}&fields=[\"price\"]&raw=true",
            200);

    searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    // EXISTS
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.*.*.price\": {\"$exists\": true}}&fields=[\"price\", \"name\", \"manufacturer\", \"model\", \"sku\"]&raw=true",
            200);
    searchResultStr =
        "["
            + "{\"products\":{\"electronics\":{\"Pixel_3a\":{\"price\":600, \"name\":\"Pixel\", \"manufacturer\":\"Google\", \"model\":\"3a\"}}}},"
            + "{\"products\":{\"electronics\":{\"iPhone_11\":{\"price\":900, \"name\":\"iPhone\", \"manufacturer\":\"Apple\", \"model\":\"11\"}}}},"
            + "{\"products\":{\"food\":{\"Apple\":{\"name\": \"apple\", \"price\":0.99, \"sku\": \"100100010101001\"}}}},"
            + "{\"products\":{\"food\":{\"Pear\":{\"name\": \"pear\", \"price\":0.89, \"sku\": null}}}}"
            + "]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));
  }

  @Test
  public void testSearchNotEquals() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // NE with String
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.*.model\": {\"$ne\": \"3a\"}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // NE with Boolean
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"quiz.nests.q1.options.[3].this\": {\"$ne\": false}}",
            200);

    searchResultStr =
        "[{\"quiz\": {\"nests\": { \"q1\": {\"options\": {\"[3]\": {\"this\": true}}}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // NE with integer compared to double
    r =
        RestUtils.get(
            authToken,
            collectionPath + "/cool-search-id?where={\"quiz.maths.q1.answer\": {\"$ne\": 12}}",
            200);

    searchResultStr = "[{\"quiz\": {\"maths\": { \"q1\": {\"answer\": 12.2}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // NE with double compared to integer
    RestUtils.get(
        authToken,
        collectionPath + "/cool-search-id?where={\"quiz.maths.q2.answer\": {\"$ne\": 4.0}}",
        204);
  }

  @Test
  public void testBasicSearchProfile() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // EQ
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?profile=true&where={\"products.electronics.Pixel_3a.price\": {\"$eq\": 600}}",
            200);
    assertThat(OBJECT_MAPPER.readTree(r).get("profile").isEmpty()).isFalse();
  }

  @Test
  public void testOrSearch() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // $OR
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id/products/food?where={\"$or\":[{\"*.name\":{\"$eq\":\"pear\"}},{\"*.name\":{\"$eq\":\"orange\"}}]}&fields=[\"name\"]",
            200);

    String searchResultStr =
        "[{\"products\": {\"food\": {\"Orange\": {\"name\": \"orange\"}}}}, {\"products\": {\"food\": {\"Pear\": {\"name\": \"pear\"}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testOrSearchWithPaging() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // $OR + page-size param
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id/products/food?where={\"$or\":[{\"*.name\":{\"$eq\":\"pear\"}},{\"*.name\":{\"$eq\":\"orange\"}}]}&fields=[\"name\"]&page-size=1",
            200);

    String searchResultStr = "[{\"products\": {\"food\": {\"Orange\": {\"name\": \"orange\"}}}}]";
    JsonNode actual = OBJECT_MAPPER.readTree(r);
    assertThat(actual.at("/data")).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));
    String pageState = actual.at("/pageState").requireNonNull().asText();
    assertThat(pageState).isNotNull();

    // paging only second with state
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id/products/food?where={\"$or\":[{\"*.name\":{\"$eq\":\"pear\"}},{\"*.name\":{\"$eq\":\"orange\"}}]}&fields=[\"name\"]&page-size=1&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);

    searchResultStr = "[{\"products\": {\"food\": {\"Pear\": {\"name\": \"pear\"}}}}]";
    actual = OBJECT_MAPPER.readTree(r);
    assertThat(actual.at("/data")).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));
  }

  @Test
  public void testSearchIn() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // IN with String
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.*.model\": {\"$in\": [\"11\", \"3a\"]}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // IN with int
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.*.*.price\": {\"$in\": [600, 900]}}",
            200);

    searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // IN with double
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.*.*.price\": {\"$in\": [0.99, 0.89]}}",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": { \"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // IN with null
    r =
        RestUtils.get(
            authToken,
            collectionPath + "/cool-search-id?where={\"products.*.*.sku\": {\"$in\": [null]}}",
            200);

    searchResultStr = "[{\"products\": {\"food\": { \"Pear\": {\"sku\": null}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testSearchNotIn() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // NIN with String
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.*.model\": {\"$nin\": [\"12\"]}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // NIN with int
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.*.*.price\": {\"$nin\": [600, 900]}}",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": { \"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // NIN with double
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.*.*.price\": {\"$nin\": [0.99, 0.89]}}",
            200);

    searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // NIN with null
    r =
        RestUtils.get(
            authToken,
            collectionPath + "/cool-search-id?where={\"products.*.*.sku\": {\"$nin\": [null]}}",
            200);

    searchResultStr = "[{\"products\": {\"food\": { \"Apple\": {\"sku\": \"100100010101001\"}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testFilterCombos() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // NIN (limited support) with GT (full support)
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.*.model\": {\"$nin\": [\"11\"], \"$gt\": \"\"}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));

    // IN (limited support) with NE (limited support)
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.electronics.*.model\": {\"$in\": [\"11\", \"3a\"], \"$ne\": \"11\"}}",
            200);

    searchResultStr = "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r))
        .isEqualTo(wrapResponse(OBJECT_MAPPER.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testInvalidSearch() throws IOException {
    // without collect initalized, we can not test below
    RestUtils.put(authToken, collectionPath + "/dummy", "{\"a\": 1}", 200);

    RestUtils.get(authToken, collectionPath + "/cool-search-id?where=hello", 400);

    String r = RestUtils.get(authToken, collectionPath + "/cool-search-id?where=[\"a\"]}", 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Search was expecting a JSON object as input.\",\"code\":400}");

    r = RestUtils.get(authToken, collectionPath + "/cool-search-id?where={\"a\": true}}", 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"A filter operation and value resolved as invalid.\",\"code\":400}");

    r =
        RestUtils.get(
            authToken, collectionPath + "/cool-search-id?where={\"a\": {\"exists\": true}}}", 400);
    assertThat(r).startsWith("{\"description\":\"Operation 'exists' is not supported.");

    r =
        RestUtils.get(
            authToken, collectionPath + "/cool-search-id?where={\"a\": {\"$eq\": null}}}", 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Operation '$eq' does not support the provided value null.\",\"code\":400}");

    r =
        RestUtils.get(
            authToken, collectionPath + "/cool-search-id?where={\"a\": {\"$eq\": {}}}}", 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Operation '$eq' does not support the provided value { }.\",\"code\":400}");

    r =
        RestUtils.get(
            authToken, collectionPath + "/cool-search-id?where={\"a\": {\"$eq\": []}}}", 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Operation '$eq' does not support the provided value [ ].\",\"code\":400}");

    r =
        RestUtils.get(
            authToken, collectionPath + "/cool-search-id?where={\"a\": {\"$in\": 2}}}", 400);
    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Operation '$in' does not support the provided value 2.\",\"code\":400}");

    r =
        RestUtils.get(
            authToken,
            collectionPath + "/cool-search-id?where={\"a\": {\"$eq\": 300}, \"b\": {\"$lt\": 500}}",
            400);
    assertThat(r)
        .contains("{\"description\":\"Conditions across multiple fields are not yet supported");

    r =
        RestUtils.get(
            authToken,
            collectionPath + "/cool-search-id?where={\"a\": {\"$in\": [1]}}&fields=[\"b\"]",
            400);
    assertThat(r)
        .contains(
            "{\"description\":\"When selecting `fields`, the field referenced by `where` must be in the selection.\",\"code\":400}");

    r = RestUtils.get(authToken, collectionPath + "/cool-search-id?page-size=0", 400);
    assertThat(r)
        .contains(
            "{\"description\":\"Request invalid: the minimum number of results to return is one.\",\"code\":400}");
  }

  @Test
  public void testMultiSearch() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // Multiple operators
    String r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"products.food.Orange.info.price\": {\"$gt\": 600, \"$lt\": 600.05}}&raw=true",
            200);

    String searchResultStr =
        "[{\"products\": {\"food\": {\"Orange\": {\"info\": {\"price\": 600.01}}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    // Array paths
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"quiz.maths.q1.options.[0]\": {\"$lt\": 13.3}}&raw=true",
            200);
    searchResultStr = "[{\"quiz\":{\"maths\":{\"q1\":{\"options\":{\"[0]\":10.2}}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"quiz.nests.q2.options.*.this.that.them\": {\"$eq\": false}}&raw=true",
            200);
    searchResultStr =
        "[{\"quiz\":{\"nests\":{\"q2\":{\"options\":{\"[3]\":{\"this\":{\"that\":{\"them\":false}}}}}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    // Multi-path
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"quiz.nests.q1,q2.options.[0]\": {\"$eq\": \"nest\"}}&raw=true",
            200);
    searchResultStr =
        "["
            + "{\"quiz\":{\"nests\":{\"q1\":{\"options\":{\"[0]\":\"nest\"}}}}},"
            + "{\"quiz\":{\"nests\":{\"q2\":{\"options\":{\"[0]\":\"nest\"}}}}}"
            + "]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));

    // Multi-path...and glob?!?
    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"quiz.nests.q2,q3.options.*.this.them\": {\"$eq\": false}}&raw=true",
            200);
    searchResultStr =
        "[{\"quiz\":{\"nests\":{\"q3\":{\"options\":{\"[2]\":{\"this\":{\"them\":false}}}}}}}]";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(searchResultStr));
  }

  @Test
  public void testPaginationSingleDocSearch() throws IOException {
    JsonNode fullObj =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj.toString(), 200);

    // With page size of 100
    String r =
        RestUtils.get(
            authToken,
            collectionPath + "/cool-search-id?page-size=20&where={\"*.value\": {\"$gt\": 0}}",
            200);
    JsonNode responseBody1 = OBJECT_MAPPER.readTree(r);

    assertThat(responseBody1.requiredAt("/data").size()).isEqualTo(20);
    String pageState = responseBody1.requiredAt("/pageState").requireNonNull().asText();

    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?page-size=20&where={\"*.value\": {\"$gt\": 0}}&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    JsonNode responseBody2 = OBJECT_MAPPER.readTree(r);

    assertThat(responseBody2.requiredAt("/data").size()).isEqualTo(20);

    JsonNode data = responseBody2.requiredAt("/data");
    Iterator<JsonNode> iter = data.iterator();

    // Response 2 (second page) should have no data matching first page
    while (iter.hasNext()) {
      JsonNode node = iter.next();
      String keyName = node.fieldNames().next();
      assertThat(responseBody1.findValue(keyName)).isNull();
    }

    // With provided page size, and a filter
    r =
        RestUtils.get(
            authToken,
            collectionPath + "/cool-search-id?where={\"*.value\": {\"$gt\": 1}}&page-size=10",
            200);
    responseBody1 = OBJECT_MAPPER.readTree(r);

    assertThat(responseBody1.requiredAt("/data").size()).isEqualTo(10);
    pageState = responseBody1.requiredAt("/pageState").requireNonNull().asText();

    r =
        RestUtils.get(
            authToken,
            collectionPath
                + "/cool-search-id?where={\"*.value\": {\"$gt\": 1}}&page-size=10&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    responseBody2 = OBJECT_MAPPER.readTree(r);

    assertThat(responseBody2.requiredAt("/data").size()).isEqualTo(10);

    data = responseBody2.requiredAt("/data");
    iter = data.iterator();

    // Response 2 (second page) should have no data matching first page
    while (iter.hasNext()) {
      JsonNode node = iter.next();
      String keyName = node.fieldNames().next();
      assertThat(responseBody1.findValue(keyName)).isNull();
    }
  }

  @Test
  public void createCollection() throws IOException {
    String tableName = "tb_createTable_" + System.currentTimeMillis();

    RestUtils.post(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections",
        "{\"name\" : \"" + tableName + "\"}",
        201);
  }

  @Test
  public void createExistingCollection() throws IOException {
    // second post fails with 409
    String tableName = "tb_createTable_" + System.currentTimeMillis();

    RestUtils.post(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections",
        "{\"name\" : \"" + tableName + "\"}",
        201);

    String response =
        RestUtils.post(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections",
            "{\"name\" : \"" + tableName + "\"}",
            409);

    assertThat(response)
        .isEqualTo(
            "{\"description\":\"Create failed: collection "
                + tableName
                + " already exists.\",\"code\":409}");
  }

  ///////////////////////////////////////
  //        Full docs search           //
  ///////////////////////////////////////

  @Test
  public void searchSinglePersistenceFilter() throws Exception {
    JsonNode matching = OBJECT_MAPPER.readTree("{\"value\": \"a\"}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode matchingWrongPath = OBJECT_MAPPER.readTree("{\"someStuff\": {\"value\": \"a\"}}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/wrong-path", matchingWrongPath.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$eq\": \"a\"}}&raw=true",
            200);

    String expected = "{\"matching\":{\"value\":\"a\"}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchSinglePersistenceFilterWithBooleanValue() throws Exception {
    JsonNode matching = OBJECT_MAPPER.readTree("{\"value\": true}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": false}");
    JsonNode matchingWrongPath = OBJECT_MAPPER.readTree("{\"someStuff\": {\"value\": true}}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/wrong-path", matchingWrongPath.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$eq\": true}}&raw=true",
            200);

    String expected = "{\"matching\":{\"value\":true}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchMultiPersistenceFilter() throws Exception {
    JsonNode matching = OBJECT_MAPPER.readTree("{\"value\": \"a\", \"n\": { \"value\": 5}}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"a\", \"n\": { \"value\": 10}}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$eq\": \"a\"}, \"n.value\": {\"$lt\": 6}}&raw=true",
            200);

    String expected = "{\"matching\":{\"value\":\"a\",\"n\":{\"value\":5}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchMultiPersistenceFilterWithSelectivity() throws Exception {
    JsonNode matching = OBJECT_MAPPER.readTree("{\"value\": \"a\", \"n\": { \"value\": 5}}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"a\", \"n\": { \"value\": 10}}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={"
                + "\"value\": {\"$eq\": \"a\"},"
                + "\"n.value\": {\"$lt\": 6, \"$selectivity\":0.5}"
                + "}&raw=true",
            200);

    String expected = "{\"matching\":{\"value\":\"a\",\"n\":{\"value\":5}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchSingleInMemoryFilter() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"value\": \"a\"}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"c\"}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/nonMatching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"value\": {\"$in\": [\"a\", \"b\"]}}&raw=true",
            200);

    String expected = "{\"matching1\":{\"value\":\"a\"},\"matching2\":{\"value\":\"b\"}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchSingleInMemoryEvaluateMissingFieldFilter() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"extra\": \"a\"}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"c\"}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/nonMatching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"value\": {\"$ne\": \"c\"}}&raw=true",
            200);

    String expected = "{\"matching1\":{\"extra\":\"a\"},\"matching2\":{\"value\":\"b\"}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchMultiInMemoryFilter() throws Exception {
    JsonNode matching = OBJECT_MAPPER.readTree("{\"value\": \"a\", \"n\": { \"value\": 5}}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"b\", \"n\": { \"value\": 10}}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$in\": [\"a\", \"b\"]}, \"n.value\": {\"$in\": [5]}}&raw=true",
            200);

    String expected = "{\"matching\":{\"value\":\"a\",\"n\":{\"value\":5}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchMultiInMemoryEvaluateMissingFieldFilter() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"value\": \"a\", \"n\": { \"value\": 5}}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"value\": \"a\"}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"b\", \"n\": { \"value\": 10}}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$in\": [\"a\", \"b\"]}, \"n.value\": {\"$nin\": [10]}}&raw=true",
            200);

    String expected =
        "{\"matching1\":{\"value\":\"a\",\"n\":{\"value\":5}},\"matching2\":{\"value\":\"a\"}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchMixedFilters() throws Exception {
    JsonNode matching = OBJECT_MAPPER.readTree("{\"value\": \"a\", \"n\": { \"value\": 5}}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"b\", \"n\": { \"value\": 10}}");
    JsonNode nonMatching2 = OBJECT_MAPPER.readTree("{\"value\": \"c\", \"n\": { \"value\": 5}}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching2", nonMatching2.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$in\": [\"a\", \"b\"]}, \"n.value\": {\"$gt\": 0, \"$lt\": 10}}&raw=true",
            200);

    String expected = "{\"matching\":{\"value\":\"a\",\"n\":{\"value\":5}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchPathMatching() throws IOException {
    JsonNode fullObj1 =
        OBJECT_MAPPER.readTree("{\"someStuff\": {\"someOtherStuff\": {\"value\": \"a\"}}}");
    JsonNode fullObj2 = OBJECT_MAPPER.readTree("{\"value\": \"a\"}");
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/cool-search-id-2", fullObj2.toString(), 200);

    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"someStuff.someOtherStuff.value\": {\"$eq\": \"a\"}}&raw=true",
            200);

    String expected =
        "{\"cool-search-id\":{\"someStuff\": {\"someOtherStuff\": {\"value\": \"a\"}}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchPathSegmentMatching() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"n\": { \"value\": 5}}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"m\": { \"value\": 8}}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"x\": { \"value\": 10}}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"n,m.value\": {\"$gte\": 5}}&raw=true",
            200);

    String expected = "{\"matching1\":{\"n\":{\"value\":5}},\"matching2\":{\"m\":{\"value\":8}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchPathSegmentInMemoryMatching() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"n\": { \"value\": 5}}");
    JsonNode nonMatching1 = OBJECT_MAPPER.readTree("{\"m\": { \"value\": 8}}");
    JsonNode nonMatching2 = OBJECT_MAPPER.readTree("{\"x\": { \"value\": 10}}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching1", nonMatching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching2", nonMatching2.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"n,m.value\": {\"$in\": [5]}}&raw=true",
            200);

    String expected = "{\"matching1\":{\"n\":{\"value\":5}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchPathWildcardsMatching() throws Exception {
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode wildcardMatching =
        OBJECT_MAPPER.readTree(
            "{\"someStuff\": {\"1\": {\"value\": \"a\"}, \"2\": {\"value\": \"b\"}}}");
    JsonNode wildcardNotMatching =
        OBJECT_MAPPER.readTree(
            "{\"someStuff\": {\"1\": {\"value\": \"c\"}, \"2\": {\"value\": \"d\"}}}");
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/wild-card", wildcardMatching.toString(), 200);
    RestUtils.put(
        authToken, collectionPath + "/wild-card-not-matching", wildcardNotMatching.toString(), 200);

    // wild card path
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"someStuff.*.value\": {\"$eq\": \"b\"}}&raw=true",
            200);

    String expected =
        "{\"wild-card\":{\"someStuff\": {\"1\": {\"value\": \"a\"}, \"2\": {\"value\": \"b\"}}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchPathWildcardsInMemoryMatching() throws Exception {
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode wildcardMatching =
        OBJECT_MAPPER.readTree(
            "{\"someStuff\": {\"1\": {\"value\": \"a\"}, \"2\": {\"value\": \"b\"}}}");
    JsonNode wildcardNotMatching =
        OBJECT_MAPPER.readTree(
            "{\"someStuff\": {\"1\": {\"value\": \"c\"}, \"2\": {\"value\": \"d\"}}}");
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/wild-card", wildcardMatching.toString(), 200);
    RestUtils.put(
        authToken, collectionPath + "/wild-card-not-matching", wildcardNotMatching.toString(), 200);

    // wild card path
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"someStuff.*.value\": {\"$in\": [\"b\"]}}&raw=true",
            200);

    String expected =
        "{\"wild-card\":{\"someStuff\": {\"1\": {\"value\": \"a\"}, \"2\": {\"value\": \"b\"}}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchPathWildcardsCandidatesMatching() throws Exception {
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode wildcardMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"a\"}, \"2\": {\"value\": \"b\"}}}");
    JsonNode wildcardNotMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"c\"}, \"2\": {\"value\": \"d\"}}}");
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/wild-card", wildcardMatching.toString(), 200);
    RestUtils.put(
        authToken, collectionPath + "/wild-card-not-matching", wildcardNotMatching.toString(), 200);

    // wild card path
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$eq\": \"b\"}, \"someStuff.*.value\": {\"$eq\": \"b\"}}&raw=true",
            200);

    String expected =
        "{\"wild-card\":{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"a\"}, \"2\": {\"value\": \"b\"}}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchPathWildcardsCandidatesInMemoryMatching() throws Exception {
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode wildcardMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"a\"}, \"2\": {\"value\": \"b\"}}}");
    JsonNode wildcardNotMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"c\"}, \"2\": {\"value\": \"d\"}}}");
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/wild-card", wildcardMatching.toString(), 200);
    RestUtils.put(
        authToken, collectionPath + "/wild-card-not-matching", wildcardNotMatching.toString(), 200);

    // wild card path
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$eq\": \"b\"}, \"someStuff.*.value\": {\"$in\": [\"b\"]}}&raw=true",
            200);

    String expected =
        "{\"wild-card\":{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"a\"}, \"2\": {\"value\": \"b\"}}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchPathWildcardsExists() throws Exception {
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode wildcardMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"a\"}, \"2\": {\"other\": \"b\"}}}");
    JsonNode wildcardNotMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"c\"}, \"2\": {\"value\": \"d\"}}}");
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/wild-card", wildcardMatching.toString(), 200);
    RestUtils.put(
        authToken, collectionPath + "/wild-card-not-matching", wildcardNotMatching.toString(), 200);

    // wild card path
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$eq\": \"b\"}, \"someStuff.*.other\": {\"$exists\": true}}&raw=true",
            200);

    String expected =
        "{\"wild-card\":{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"a\"}, \"2\": {\"other\": \"b\"}}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchArrayPathMatching() throws Exception {
    JsonNode matching = OBJECT_MAPPER.readTree("{\"value\": [{ \"n\": { \"value\": 5} }]}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": [{ \"n\": { \"value\": 10} }]}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value.[0].n.value\": {\"$eq\": 5}}&raw=true",
            200);

    String expected = "{\"matching\":{\"value\":[{\"n\":{\"value\":5}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchArrayPathSegmentMatching() throws Exception {
    JsonNode matching1 =
        OBJECT_MAPPER.readTree(
            "{\"value\": [{ \"n\": { \"value\": 5} }, { \"n\": { \"value\": 8} }]}");
    JsonNode matching2 =
        OBJECT_MAPPER.readTree(
            "{\"value\": [{ \"n\": { \"value\": 10} }, { \"n\": { \"value\": 3} }]}");
    JsonNode nonMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": [{ \"n\": { \"value\": 10} },{ \"n\": { \"value\": 20} },{ \"n\": { \"value\": 2} }]}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"value.[0],[1].n.value\": {\"$lt\": 6}}&raw=true",
            200);

    String expected =
        "{\"matching1\":{\"value\":[{\"n\":{\"value\":5}},{\"n\":{\"value\":8}}]},\"matching2\":{\"value\":[{\"n\":{\"value\":10}},{\"n\":{\"value\":3}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchArrayPathSegmentInMemoryMatching() throws Exception {
    JsonNode matching1 =
        OBJECT_MAPPER.readTree(
            "{\"value\": [{ \"n\": { \"value\": 5} }, { \"n\": { \"value\": 8} }]}");
    JsonNode nonMatching1 =
        OBJECT_MAPPER.readTree(
            "{\"value\": [{ \"n\": { \"value\": 10} }, { \"n\": { \"value\": 3} }]}");
    JsonNode nonMatching2 =
        OBJECT_MAPPER.readTree(
            "{\"value\": [{ \"n\": { \"value\": 10} },{ \"n\": { \"value\": 20} },{ \"n\": { \"value\": 2} }]}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching1", nonMatching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching2", nonMatching2.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"value.[0],[1].n.value\": {\"$in\": [8]}}&raw=true",
            200);

    String expected = "{\"matching1\":{\"value\":[{\"n\":{\"value\":5}},{\"n\":{\"value\":8}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchArrayWildcardMatching() throws Exception {
    JsonNode matching =
        OBJECT_MAPPER.readTree(
            "{\"value\": [{ \"n\": { \"value\": 5} }, { \"n\": { \"value\": 8} }]}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": [{ \"n\": { \"value\": 10} }]}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value.[*].n.value\": {\"$eq\": 8}}&raw=true",
            200);

    String expected = "{\"matching\":{\"value\":[{\"n\":{\"value\":5}},{\"n\":{\"value\":8}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchArrayWildcardCandidatesMatching() throws Exception {
    JsonNode matching =
        OBJECT_MAPPER.readTree(
            "{\"first\": 5, \"value\": [{ \"n\": { \"value\": 5} }, { \"n\": { \"value\": 8} }]}");
    JsonNode nonMatching =
        OBJECT_MAPPER.readTree("{\"first\": 50, \"value\": [{ \"n\": { \"value\": 10} }]}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"first\": {\"$gt\": 0}, \"value.[*].n.value\": {\"$eq\": 8}}&raw=true",
            200);

    String expected =
        "{\"matching\":{\"first\": 5, \"value\":[{\"n\":{\"value\":5}},{\"n\":{\"value\":8}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchArrayWildcardInMemoryMatching() throws Exception {
    JsonNode matching =
        OBJECT_MAPPER.readTree(
            "{\"value\": [{ \"n\": { \"value\": 5} }, { \"n\": { \"value\": 8} }]}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": [{ \"n\": { \"value\": 10} }]}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value.[*].n.value\": {\"$in\": [8]}}&raw=true",
            200);

    String expected = "{\"matching\":{\"value\":[{\"n\":{\"value\":5}},{\"n\":{\"value\":8}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchArrayWildcardCandidatesInMemoryMatching() throws Exception {
    JsonNode matching =
        OBJECT_MAPPER.readTree(
            "{\"first\": 5, \"value\": [{ \"n\": { \"value\": 5} }, { \"n\": { \"value\": 8} }]}");
    JsonNode nonMatching =
        OBJECT_MAPPER.readTree("{\"first\": 1, \"value\": [{ \"n\": { \"value\": 10} }]}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"first\": {\"$gte\": 1}, \"value.[*].n.value\": {\"$in\": [8]}}&raw=true",
            200);

    String expected =
        "{\"matching\":{\"first\": 5, \"value\":[{\"n\":{\"value\":5}},{\"n\":{\"value\":8}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchArrayWildcardExists() throws Exception {
    JsonNode matching =
        OBJECT_MAPPER.readTree(
            "{\"first\": 5, \"value\": [{ \"n\": { \"value\": 5} }, { \"m\": { \"value\": 8} }]}");
    JsonNode nonMatching =
        OBJECT_MAPPER.readTree("{\"first\": 1, \"value\": [{ \"n\": { \"value\": 10} }]}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"first\": {\"$gte\": 1}, \"value.[*].m.value\": {\"$exists\": true}}&raw=true",
            200);

    String expected =
        "{\"matching\":{\"first\": 5, \"value\":[{\"n\":{\"value\":5}},{\"m\":{\"value\":8}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchOrPersistenceFilter() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"value\": \"a\"}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"c\"}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/nonMatching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"$or\": [{\"value\": {\"$eq\": \"a\"}}, {\"value\": {\"$eq\": \"b\"}}]}&raw=true",
            200);

    String expected = "{\"matching1\":{\"value\":\"a\"},\"matching2\":{\"value\":\"b\"}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchOrInMemoryFilter() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"value\": \"a\"}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"c\"}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/nonMatching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"$or\": [{\"value\": {\"$in\": [\"a\"]}}, {\"value\": {\"$in\": [\"b\"]}}]}&raw=true",
            200);

    String expected = "{\"matching1\":{\"value\":\"a\"},\"matching2\":{\"value\":\"b\"}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchOrMixedFilter() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"value\": \"a\"}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"c\"}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/nonMatching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"$or\": [{\"value\": {\"$eq\": \"a\"}}, {\"value\": {\"$in\": [\"b\"]}}]}&raw=true",
            200);

    String expected = "{\"matching1\":{\"value\":\"a\"},\"matching2\":{\"value\":\"b\"}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchOrMixedFilterWithPaging() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"value\": \"a\"}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"c\"}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/nonMatching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=1&where={\"$or\": [{\"value\": {\"$eq\": \"a\"}}, {\"value\": {\"$in\": [\"b\"]}}]}",
            200);

    String expected = "{\"matching1\":{\"value\":\"a\"}}";
    JsonNode result = OBJECT_MAPPER.readTree(r);
    assertThat(result.at("/data")).isEqualTo(OBJECT_MAPPER.readTree(expected));
    assertThat(result.at("/pageState")).isNotNull();
    String pageState = result.at("/pageState").requireNonNull().asText();
    assertThat(pageState).isNotNull();

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=1&where={\"$or\": [{\"value\": {\"$eq\": \"a\"}}, {\"value\": {\"$in\": [\"b\"]}}]}&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);

    expected = "{\"matching2\":{\"value\":\"b\"}}";
    result = OBJECT_MAPPER.readTree(r);
    assertThat(result.at("/data")).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchOrMixedFiltersDifferentPaths() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"value\": \"a\", \"count\": 1}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"value\": \"b\", \"count\": 2}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"c\", \"count\": 3}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/nonMatching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"$or\": [{\"value\": {\"$eq\": \"a\"}}, {\"count\": {\"$in\": [2,4]}}]}&raw=true",
            200);

    String expected =
        "{\"matching1\":{\"value\":\"a\",\"count\": 1},\"matching2\":{\"value\":\"b\",\"count\": 2}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchAndWithOr() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"value\": \"a\", \"count\": 1}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"value\": \"b\", \"count\": 2}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"c\", \"count\": 3}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/nonMatching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"count\": {\"$gt\": 0}, \"$or\": [{\"value\": {\"$eq\": \"a\"}}, {\"value\": {\"$in\": [\"b\"]}}]}&raw=true",
            200);

    String expected =
        "{\"matching1\":{\"value\":\"a\",\"count\": 1},\"matching2\":{\"value\":\"b\",\"count\": 2}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchOrExists() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"value\": \"a\"}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"other\": \"b\"}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"c\"}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/nonMatching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"$or\": [{\"value\": {\"$eq\": \"a\"}}, {\"other\": {\"$exists\": true}}]}&raw=true",
            200);

    String expected = "{\"matching1\":{\"value\":\"a\"},\"matching2\":{\"other\":\"b\"}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchOrEvaluateOnMissing() throws Exception {
    JsonNode matching1 = OBJECT_MAPPER.readTree("{\"value\": \"a\"}");
    JsonNode matching2 = OBJECT_MAPPER.readTree("{\"other\": \"b\"}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": \"c\"}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/nonMatching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"$or\": [{\"value\": {\"$nin\": [\"b\",\"c\"]}}, {\"value\": {\"$eq\": \"a\"}}]}&raw=true",
            200);

    String expected = "{\"matching1\":{\"value\":\"a\"},\"matching2\":{\"other\":\"b\"}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchOrPathSegmentsAndWildcards() throws Exception {
    JsonNode matching1 =
        OBJECT_MAPPER.readTree(
            "{\"value\": [{ \"n\": { \"value\": 5} }, { \"m\": { \"value\": 8} }]}");
    JsonNode matching2 =
        OBJECT_MAPPER.readTree(
            "{\"value\": [{ \"x\": { \"value\": 10} }, { \"y\": { \"value\": 20} }]}");
    JsonNode nonMatching = OBJECT_MAPPER.readTree("{\"value\": [{ \"n\": { \"value\": 10} }]}");
    RestUtils.put(authToken, collectionPath + "/matching1", matching1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/matching2", matching2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/non-matching", nonMatching.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"$or\": [{\"value.[*].*.value\": {\"$eq\": 20}}, {\"value.[1],[2].n,m.value\": {\"$eq\": 8}}]}&raw=true",
            200);

    String expected =
        "{\"matching1\":{\"value\":[{\"n\":{\"value\":5}},{\"m\":{\"value\":8}}]},\"matching2\":{\"value\":[{\"x\":{\"value\":10}},{\"y\":{\"value\":20}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchWithProfile() throws IOException {
    JsonNode fullObj1 =
        OBJECT_MAPPER.readTree("{\"someStuff\": {\"someOtherStuff\": {\"value\": \"a\"}}}");
    RestUtils.put(authToken, collectionPath + "/cool-search-id", fullObj1.toString(), 200);

    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$eq\": \"a\"}}&raw=false&profile=true",
            200);
    assertThat(OBJECT_MAPPER.readTree(r).get("profile").isEmpty()).isFalse();
  }

  @Test
  public void searchWithFieldsArrayPath() throws Exception {
    JsonNode doc =
        OBJECT_MAPPER.readTree(
            "{\"first\": 5, \"value\": [{ \"n\": { \"value\": 5} }, { \"m\": { \"value\": 8} }]}");
    RestUtils.put(authToken, collectionPath + "/doc", doc.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&fields=[\"value.[1].m\"]&raw=true",
            200);

    String expected = "{\"doc\":{\"value\":[null,{\"m\":{\"value\":8}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchWithFieldsArrayGlobSegmentedPath() throws Exception {
    JsonNode doc =
        OBJECT_MAPPER.readTree(
            "{\"first\": 5, \"value\": [{ \"n\": { \"value\": 5} }, { \"m\": { \"value\": 8} }]}");
    RestUtils.put(authToken, collectionPath + "/doc", doc.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&fields=[\"value.[*].m,n\"]&raw=true",
            200);

    String expected = "{\"doc\":{\"value\":[{\"n\":{\"value\":5}},{\"m\":{\"value\":8}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchWithFieldsDoubleGlob() throws Exception {
    JsonNode doc =
        OBJECT_MAPPER.readTree(
            "{\"first\": 5, \"value\": [{ \"n\": { \"value\": 5} }, { \"m\": { \"value\": 8} }]}");
    RestUtils.put(authToken, collectionPath + "/doc", doc.toString(), 200);

    // Any filter on full collection search should only match the level of nesting of the where
    // clause
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&fields=[\"*.[*].n\"]&raw=true",
            200);

    String expected = "{\"doc\":{\"value\":[{\"n\":{\"value\":5}}]}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchExistsFalse() throws Exception {
    JsonNode matching = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode wildcardMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": \"b\", \"someStuff\": {\"1\": {\"first\": \"a\"}, \"2\": {\"second\": \"b\"}}}");
    JsonNode wildcardNotMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"c\"}, \"2\": {\"value\": \"d\"}}}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/wild-card", wildcardMatching.toString(), 200);
    RestUtils.put(
        authToken, collectionPath + "/wild-card-not-matching", wildcardNotMatching.toString(), 200);

    // wild card path
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"someStuff.*.value\": {\"$exists\": false}}&raw=true",
            200);

    String expected =
        "{\"matching\":{\"value\": \"b\"}, \"wild-card\":{\"value\": \"b\", \"someStuff\": {\"1\": {\"first\": \"a\"}, \"2\": {\"second\": \"b\"}}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchExistsFalseMixed() throws Exception {
    JsonNode matching = OBJECT_MAPPER.readTree("{\"value\": \"b\"}");
    JsonNode wildcardMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": \"b\", \"someStuff\": {\"1\": {\"first\": \"a\"}, \"2\": {\"second\": \"b\"}}}");
    JsonNode wildcardNotMatching =
        OBJECT_MAPPER.readTree(
            "{\"value\": \"b\", \"someStuff\": {\"1\": {\"value\": \"c\"}, \"2\": {\"value\": \"d\"}}}");
    RestUtils.put(authToken, collectionPath + "/matching", matching.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/wild-card", wildcardMatching.toString(), 200);
    RestUtils.put(
        authToken, collectionPath + "/wild-card-not-matching", wildcardNotMatching.toString(), 200);

    // wild card path
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"value\": {\"$eq\": \"b\"}, \"someStuff.*.value\": {\"$exists\": false}}&raw=true",
            200);

    String expected =
        "{\"matching\":{\"value\": \"b\"}, \"wild-card\":{\"value\": \"b\", \"someStuff\": {\"1\": {\"first\": \"a\"}, \"2\": {\"second\": \"b\"}}}}";
    assertThat(OBJECT_MAPPER.readTree(r)).isEqualTo(OBJECT_MAPPER.readTree(expected));
  }

  @Test
  public void searchInvalidPageSize() throws IOException {
    RestUtils.put(authToken, collectionPath + "/dummy", "{\"k\":\"v\"}", 200);

    String r = RestUtils.get(authToken, collectionPath + "?page-size=-1", 400);

    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Request invalid: the minimum number of documents to return is one.\",\"code\":400}");

    r = RestUtils.get(authToken, collectionPath + "?page-size=21", 400);

    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Request invalid: the max number of documents to return is 20.\",\"code\":400}");
  }

  // below are tests before move to the reactive search service, keeping for more safety

  @Test
  public void testGetFullDocMultiFilter() throws IOException {
    JsonNode doc =
        OBJECT_MAPPER.readTree(
            "{\"a\": \"b\", \"c\": 2, \"quiz\": {\"sport\": {\"q1\": {\"question\": \"Which one is correct team name in NBA?\"}}}}");
    JsonNode doc2 =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));

    RestUtils.put(authToken, collectionPath + "/1", doc.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/2", doc2.toString(), 200);

    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"a\":{\"$eq\":\"b\"},\"c\":{\"$lt\":3}}",
            200);

    JsonNode resp = OBJECT_MAPPER.readTree(r);
    JsonNode data = resp.requiredAt("/data");
    assertThat(data.requiredAt("/1")).isEqualTo(doc);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"a\":{\"$eq\":\"b\"},\"c\":{\"$lt\":0}}",
            200);

    // resulting JSON should be empty
    resp = OBJECT_MAPPER.readTree(r);
    data = resp.requiredAt("/data");

    JsonNode expected = OBJECT_MAPPER.createObjectNode();
    assertThat(data).isEqualTo(expected);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=3&where={\"quiz.sport.q1.question\":{\"$in\": [\"Which one is correct team name in NBA?\"]}}",
            200);

    resp = OBJECT_MAPPER.readTree(r);
    data = resp.requiredAt("/data");
    assertThat(data.requiredAt("/1")).isEqualTo(doc);
    assertThat(data.requiredAt("/2")).isEqualTo(doc2);
  }

  @Test
  public void testPaginationGetFullDoc() throws IOException {
    JsonNode doc1 =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    JsonNode doc2 = OBJECT_MAPPER.readTree("{\"a\": \"b\"}");
    JsonNode doc3 =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));

    ObjectNode docsByKey = OBJECT_MAPPER.createObjectNode();
    docsByKey.set("1", doc1);
    docsByKey.set("2", doc2);
    docsByKey.set("3", doc3);

    Set<String> docsSeen = new HashSet<>();

    RestUtils.put(authToken, collectionPath + "/1", doc1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/2", doc2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/3", doc3.toString(), 200);

    String r = RestUtils.get(authToken, collectionPath + "?page-size=1", 200);
    JsonNode resp = OBJECT_MAPPER.readTree(r);
    String pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    JsonNode data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    String key = data.fieldNames().next();
    assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    docsSeen.add(key);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=1&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = OBJECT_MAPPER.readTree(r);
    pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    key = data.fieldNames().next();
    assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    docsSeen.add(key);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=1&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = OBJECT_MAPPER.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    key = data.fieldNames().next();
    assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    docsSeen.add(key);

    // Make sure we've seen every key
    assertThat(docsSeen.size()).isEqualTo(3);

    docsSeen = new HashSet<>();
    // set page-size to 2
    r = RestUtils.get(authToken, collectionPath + "?page-size=2", 200);
    resp = OBJECT_MAPPER.readTree(r);
    pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(2);
    // Any two documents could come back, find out which are there
    Iterator<String> fieldNames = data.fieldNames();
    key = fieldNames.next();
    assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    docsSeen.add(key);
    key = fieldNames.next();
    assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    docsSeen.add(key);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = OBJECT_MAPPER.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    key = data.fieldNames().next();
    assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    docsSeen.add(key);

    // Make sure we've seen every key
    assertThat(docsSeen.size()).isEqualTo(3);

    docsSeen = new HashSet<>();

    // set page-size to 4
    r = RestUtils.get(authToken, collectionPath + "?page-size=4", 200);
    resp = OBJECT_MAPPER.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(3);
    Iterator<String> iter = data.fieldNames();
    while (iter.hasNext()) {
      String field = iter.next();
      docsSeen.add(field);
      JsonNode node = data.requiredAt("/" + field);
      assertThat(node).isEqualTo(docsByKey.requiredAt("/" + field));
    }

    // Make sure we've seen every key
    assertThat(docsSeen.size()).isEqualTo(3);
  }

  @Test
  public void testPaginationGetFullDocWithFields() throws IOException {
    JsonNode doc1 =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    JsonNode doc2 = OBJECT_MAPPER.readTree("{\"a\": \"b\"}");
    JsonNode doc3 =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));

    ObjectNode docsByKey = OBJECT_MAPPER.createObjectNode();
    docsByKey.set("1", doc1);
    docsByKey.set("2", doc2);
    docsByKey.set("3", doc3);

    Set<String> docsSeen = new HashSet<>();

    RestUtils.put(authToken, collectionPath + "/1", doc1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/2", doc2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/3", doc3.toString(), 200);

    String r = RestUtils.get(authToken, collectionPath + "?page-size=1&fields=[\"a\"]", 200);
    JsonNode resp = OBJECT_MAPPER.readTree(r);
    String pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    JsonNode data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    String key = data.fieldNames().next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(OBJECT_MAPPER.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(OBJECT_MAPPER.readTree("{}"));
    }

    docsSeen.add(key);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=1&fields=[\"a\"]&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = OBJECT_MAPPER.readTree(r);
    pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    key = data.fieldNames().next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(OBJECT_MAPPER.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(OBJECT_MAPPER.readTree("{}"));
    }
    docsSeen.add(key);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=1&fields=[\"a\"]&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = OBJECT_MAPPER.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    key = data.fieldNames().next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(OBJECT_MAPPER.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(OBJECT_MAPPER.readTree("{}"));
    }
    docsSeen.add(key);

    // Make sure we've seen every key
    assertThat(docsSeen.size()).isEqualTo(3);

    docsSeen = new HashSet<>();
    // set page-size to 2
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?fields=[\"a\"]&page-size=2",
            200);
    resp = OBJECT_MAPPER.readTree(r);
    pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(2);
    // Any two documents could come back, find out which are there
    Iterator<String> fieldNames = data.fieldNames();
    key = fieldNames.next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(OBJECT_MAPPER.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(OBJECT_MAPPER.readTree("{}"));
    }
    docsSeen.add(key);
    key = fieldNames.next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(OBJECT_MAPPER.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(OBJECT_MAPPER.readTree("{}"));
    }
    docsSeen.add(key);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?fields=[\"a\"]&page-size=2&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = OBJECT_MAPPER.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    key = data.fieldNames().next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(OBJECT_MAPPER.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(OBJECT_MAPPER.readTree("{}"));
    }
    docsSeen.add(key);

    // Make sure we've seen every key
    assertThat(docsSeen.size()).isEqualTo(3);

    docsSeen = new HashSet<>();

    // set page-size to 4
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?fields=[\"a\"]&page-size=4",
            200);
    resp = OBJECT_MAPPER.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(3);
    Iterator<String> iter = data.fieldNames();
    while (iter.hasNext()) {
      String field = iter.next();
      docsSeen.add(field);
      JsonNode node = data.requiredAt("/" + field);
      if (field.equals("1")) {
        assertThat(node).isEqualTo(OBJECT_MAPPER.readTree("{\"a\": {\"value\": 1}}"));
      } else if (field.equals("2")) {
        assertThat(node).isEqualTo(docsByKey.requiredAt("/" + field));
      } else {
        assertThat(node).isEqualTo(OBJECT_MAPPER.readTree("{}"));
      }
    }

    // Make sure we've seen every key
    assertThat(docsSeen.size()).isEqualTo(3);
  }

  @Test
  public void testSearchFullDocWithNestedFields() throws IOException {
    JsonNode doc1 =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));

    RestUtils.put(authToken, collectionPath + "/1", doc1.toString(), 200);

    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?fields=[\"a.value\",\"b.value\",\"bb.value\"]",
            200);
    JsonNode resp = OBJECT_MAPPER.readTree(r);
    JsonNode data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);

    assertThat(data.requiredAt("/1"))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "{\"a\": {\"value\": 1},\"b\": {\"value\": 2}, \"bb\": {\"value\": 4}}"));
  }

  @Test
  public void testErrorCasesPushPopFunction() throws IOException {
    JsonNode doc1 =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/1", doc1.toString(), 200);

    // Try to hit function endpoint with no operation provided
    String currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/function",
            "{\"value\": \"new_value\"}",
            422);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "{\"description\":\"Request invalid: a valid `operation` is required.\",\"code\":422}"));

    // Try to hit function endpoint with invalid operation provided
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/function",
            "{\"operation\":\"$madeupfunction\",\"value\": \"new_value\"}",
            400);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "{\"description\":\"No BuiltInApiFunction found for name: $madeupfunction\",\"code\":400}"));

    // Try to push to an array with no value provided
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/function",
            "{\"operation\":\"$push\"}",
            400);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "{\"description\":\"Provided value must not be null\",\"code\":400}"));

    // Try to push to a non-array
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/function",
            "{\"operation\": \"$push\", \"value\": \"new_value\"}",
            400);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "{\"description\":\"The path provided to push to has no array\",\"code\":400}"));

    // Try to push to a non-existent path
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/randomword/function",
            "{\"operation\": \"$push\", \"value\": \"new_value\"}",
            404);

    // Try to pop from a non-array
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/function",
            "{\"operation\": \"$pop\"}",
            400);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "{\"description\":\"The path provided to pop from has no array\",\"code\":400}"));

    // Try to pop from a non-existent path
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/randomword/function",
            "{\"operation\": \"$pop\"}",
            404);
  }

  @Test
  public void testBuiltInPushPopFunction() throws IOException {
    JsonNode doc1 =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(authToken, collectionPath + "/1", doc1.toString(), 200);

    // Push some data to one of the arrays
    // A string
    RestUtils.post(
        authToken,
        collectionPath + "/1/quiz/sport/q1/options/function",
        "{\"operation\": \"$push\", \"value\": \"new_value\"}",
        200);
    String currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\"]"));

    // An integer
    RestUtils.post(
        authToken,
        collectionPath + "/1/quiz/sport/q1/options/function",
        "{\"operation\": \"$push\", \"value\": 123}",
        200);
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\",123]"));

    // A double
    RestUtils.post(
        authToken,
        collectionPath + "/1/quiz/sport/q1/options/function",
        "{\"operation\": \"$push\", \"value\": 99.99}",
        200);
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\",123,99.99]"));

    // A boolean
    RestUtils.post(
        authToken,
        collectionPath + "/1/quiz/sport/q1/options/function",
        "{\"operation\": \"$push\", \"value\": false}",
        200);
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\",123,99.99,false]"));

    // An object
    RestUtils.post(
        authToken,
        collectionPath + "/1/quiz/sport/q1/options/function",
        "{\"operation\": \"$push\", \"value\": {\"a\":\"b\"}}",
        200);
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\",123,99.99,false,{\"a\":\"b\"}]"));

    // An array
    RestUtils.post(
        authToken,
        collectionPath + "/1/quiz/sport/q1/options/function",
        "{\"operation\": \"$push\", \"value\": [1, true, \"a\"]}",
        200);
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\",123,99.99,false,{\"a\":\"b\"},[1,true,\"a\"]]"));

    // Now pop them off one by one
    // The array
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/options/function",
            "{\"operation\": \"$pop\"}",
            200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc).requiredAt("/data"))
        .isEqualTo(OBJECT_MAPPER.readTree("[1,true,\"a\"]"));
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\",123,99.99,false,{\"a\":\"b\"}]"));

    // The object
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/options/function",
            "{\"operation\": \"$pop\"}",
            200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc).requiredAt("/data"))
        .isEqualTo(OBJECT_MAPPER.readTree("{\"a\":\"b\"}"));
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\",123,99.99,false]"));

    // The boolean
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/options/function",
            "{\"operation\": \"$pop\"}",
            200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc).requiredAt("/data"))
        .isEqualTo(OBJECT_MAPPER.readTree("false"));
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\",123,99.99]"));

    // The double
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/options/function",
            "{\"operation\": \"$pop\"}",
            200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc).requiredAt("/data"))
        .isEqualTo(OBJECT_MAPPER.readTree("99.99"));
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\",123]"));

    // The integer
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/options/function",
            "{\"operation\": \"$pop\"}",
            200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc).requiredAt("/data"))
        .isEqualTo(OBJECT_MAPPER.readTree("123"));
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\",\"new_value\"]"));

    // The string
    currentDoc =
        RestUtils.post(
            authToken,
            collectionPath + "/1/quiz/sport/q1/options/function",
            "{\"operation\": \"$pop\"}",
            200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc).requiredAt("/data"))
        .isEqualTo(OBJECT_MAPPER.readTree("\"new_value\""));
    currentDoc =
        RestUtils.get(authToken, collectionPath + "/1/quiz/sport/q1/options?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\"]"));
  }

  @Test
  public void testBuiltInPushPopFunctionWithNestedArray() throws IOException {
    String json = "[{\"array\":[]}]";
    JsonNode doc1 = OBJECT_MAPPER.readTree(json);
    RestUtils.put(authToken, collectionPath + "/1", doc1.toString(), 200);

    // Push some data to the nested array
    // A string
    RestUtils.post(
        authToken,
        collectionPath + "/1/[0]/array/function",
        "{\"operation\": \"$push\", \"value\": \"new_value\"}",
        200);
    String currentDoc = RestUtils.get(authToken, collectionPath + "/1?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc))
        .isEqualTo(OBJECT_MAPPER.readTree("[{\"array\":[\"new_value\"]}]"));

    // then pop string
    String popped =
        RestUtils.post(
            authToken, collectionPath + "/1/[0]/array/function", "{\"operation\": \"$pop\"}", 200);
    assertThat(OBJECT_MAPPER.readTree(popped).requiredAt("/data"))
        .isEqualTo(OBJECT_MAPPER.readTree("\"new_value\""));

    currentDoc = RestUtils.get(authToken, collectionPath + "/1?raw=true", 200);
    assertThat(OBJECT_MAPPER.readTree(currentDoc)).isEqualTo(OBJECT_MAPPER.readTree(json));
  }

  @Test
  public void testPaginationFilterDocWithFields() throws IOException {
    JsonNode doc1 =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    JsonNode doc2 =
        OBJECT_MAPPER.readTree(
            "{\"a\": \"b\", \"quiz\": {\"sport\": {\"q1\": {\"question\": \"hello?\"}}}}");
    JsonNode doc3 =
        OBJECT_MAPPER.readTree(this.getClass().getClassLoader().getResource("example.json"));

    ObjectNode docsByKey = OBJECT_MAPPER.createObjectNode();
    docsByKey.set("1", doc1);
    docsByKey.set("2", doc2);
    docsByKey.set("3", doc3);

    Set<String> docsSeen = new HashSet<>();

    RestUtils.put(authToken, collectionPath + "/1", doc1.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/2", doc2.toString(), 200);
    RestUtils.put(authToken, collectionPath + "/3", doc3.toString(), 200);

    // page-size defaults to 1 document when excluded
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"b.value\": {\"$eq\": 2}}&fields=[\"a\"]",
            200);
    JsonNode resp = OBJECT_MAPPER.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    JsonNode data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // The only matching document based on `where` is document 1
    String key = data.fieldNames().next();
    assertThat(key).isEqualTo("1");
    assertThat(data.requiredAt("/" + key))
        .isEqualTo(OBJECT_MAPPER.readTree("{\"a\": {\"value\": 1}}"));

    // Return all documents where quiz.sport.q1.question exists, but only the `quiz` field on each
    // doc
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=1&where={\"quiz.sport.q1.question\": {\"$exists\": true}}&fields=[\"quiz\"]",
            200);
    resp = OBJECT_MAPPER.readTree(r);
    String pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // The document could either be document 2 or document 3
    key = data.fieldNames().next();
    if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(
              OBJECT_MAPPER.readTree(
                  "{\"quiz\": {\"sport\": {\"q1\": {\"question\": \"hello?\"}}}}"));
    } else {
      ObjectNode expected = OBJECT_MAPPER.createObjectNode();
      expected.set("quiz", doc3.requiredAt("/quiz"));
      assertThat(data.requiredAt("/" + key)).isEqualTo(expected);
    }
    docsSeen.add(key);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=1&where={\"quiz.sport.q1.question\": {\"$exists\": true}}&fields=[\"quiz\"]&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = OBJECT_MAPPER.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    key = data.fieldNames().next();
    if (docsSeen.contains("3")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(
              OBJECT_MAPPER.readTree(
                  "{\"quiz\": {\"sport\": {\"q1\": {\"question\": \"hello?\"}}}}"));
    } else if (docsSeen.contains("2")) {
      ObjectNode expected = OBJECT_MAPPER.createObjectNode();
      expected.set("quiz", doc3.requiredAt("/quiz"));
      assertThat(data.requiredAt("/" + key)).isEqualTo(expected);
    }
    docsSeen.add(key);
    assertThat(docsSeen.size()).isEqualTo(2);
    assertThat(docsSeen.containsAll(Arrays.asList("2", "3"))).isTrue();
  }

  private JsonNode wrapResponse(JsonNode node, String id, String pagingState) {
    ObjectNode wrapperNode = OBJECT_MAPPER.createObjectNode();

    if (id != null) {
      wrapperNode.set("documentId", TextNode.valueOf(id));
    }
    if (node != null) {
      wrapperNode.set("data", node);
    }
    if (pagingState != null) {
      wrapperNode.set("pageState", TextNode.valueOf(pagingState));
    }
    return wrapperNode;
  }
}
