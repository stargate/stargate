package io.stargate.it.http.docsapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.google.common.collect.ImmutableList;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.web.models.Keyspace;
import io.stargate.web.models.ResponseWrapper;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@NotThreadSafe
public class BaseDocumentApiV2Test extends BaseOsgiIntegrationTest {
  private String keyspace;
  private CqlSession session;
  private static String authToken;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final OkHttpClient client =
      new OkHttpClient().newBuilder().readTimeout(Duration.ofMinutes(3)).build();

  private String host;
  private String hostWithPort;

  @BeforeEach
  public void setup(StargateConnectionInfo cluster) throws IOException {
    host = "http://" + cluster.seedAddress();
    hostWithPort = host + ":8082";

    keyspace = "ks_docs_" + System.currentTimeMillis();
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
            .addContactPoint(new InetSocketAddress(cluster.seedAddress(), 9043))
            .withLocalDatacenter(cluster.datacenter())
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

  @AfterEach
  public void teardown() {
    session.execute(String.format("drop keyspace %s", keyspace));
    session.close();
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

  @Test
  public void testIt() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));

    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    String resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/maths",
            200);
    assertThat(objectMapper.readTree(resp))
        .isEqualTo(wrapResponse(obj.requiredAt("/quiz/maths"), "1", null));

    resp =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"products.electronics.Pixel_3a.price\":{\"$lt\": 800}}",
            200);
    ObjectNode expected = objectMapper.createObjectNode();
    expected.set("1", obj);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(expected, null, null));
  }

  @Test
  public void testUnauthorized() throws IOException {
    String data =
        objectMapper
            .readTree(this.getClass().getClassLoader().getResource("example.json"))
            .toString();

    // Missing token header
    RestUtils.post(
        null, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection", data, 401);
    RestUtils.put(
        null, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1", data, 401);
    RestUtils.patch(
        null, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1", data, 401);
    RestUtils.delete(
        null, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1", 401);
    RestUtils.get(
        null, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1", 401);
    RestUtils.get(
        null, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection", 401);

    // Bad token header
    RestUtils.post(
        "garbage",
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection",
        data,
        401);
    RestUtils.put(
        "garbage",
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        data,
        401);
    RestUtils.patch(
        "garbage",
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        data,
        401);
    RestUtils.delete(
        "garbage", hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1", 401);
    RestUtils.get(
        "garbage", hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1", 401);
    RestUtils.get(
        "garbage", hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection", 401);
  }

  @Test
  public void testBasicForms() throws IOException {
    RestUtils.putForm(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        "a=b&b=null&c.b=3.3&d.[0].[2]=true",
        200);

    String resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    JsonNode expected =
        objectMapper.readTree(
            "{\"a\":\"b\", \"b\":null, \"c\":{\"b\": 3.3}, \"d\":[[null, null, true]]}");
    assertThat(objectMapper.readTree(resp).toString())
        .isEqualTo(wrapResponse(expected, "1", null).toString());
  }

  @Test
  public void testInvalidKeyspaceAndTable() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String data = obj.toString();
    String resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/unknown_keyspace_1337/collections/collection/1",
            data,
            400);
    assertThat(resp)
        .isEqualTo("Unknown namespace unknown_keyspace_1337, you must create it first.");

    resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/invalid-character/1",
            data,
            400);
    assertThat(resp)
        .isEqualTo(
            "Could not create collection invalid-character, it has invalid characters. Valid characters are alphanumeric and underscores.");
  }

  @Test
  public void testInvalidKeyPut() throws IOException {
    JsonNode obj = objectMapper.readTree("{ \"square[]braces\": \"are not allowed\" }");

    String resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            400);
    assertThat(resp)
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field square[]braces");

    obj = objectMapper.readTree("{ \"commas,\": \"are not allowed\" }");
    resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            400);
    assertThat(resp)
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field commas,");

    obj = objectMapper.readTree("{ \"periods.\": \"are not allowed\" }");
    resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            400);
    assertThat(resp)
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field periods.");

    obj = objectMapper.readTree("{ \"'quotes'\": \"are not allowed\" }");
    resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            400);
    assertThat(resp)
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field 'quotes'");

    obj = objectMapper.readTree("{ \"*asterisks*\": \"are not allowed\" }");
    resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            400);
    assertThat(resp)
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field *asterisks*");
  }

  @Test
  public void testWeirdButAllowedKeys() throws IOException {
    JsonNode obj = objectMapper.readTree("{ \"$\": \"weird but allowed\" }");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
        obj.toString(),
        200);

    String resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("{ \"$30\": \"not as weird\" }");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("{ \"@\": \"weird but allowed\" }");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("{ \"meet me @ the place\": \"not as weird\" }");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("{ \"?\": \"weird but allowed\" }");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("{ \"spac es\": \"weird but allowed\" }");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("{ \"3\": [\"totally allowed\"] }");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path/3/[0]",
            200);
    assertThat(objectMapper.readTree(resp))
        .isEqualTo(wrapResponse(objectMapper.readTree("\"totally allowed\""), "1", null));

    obj = objectMapper.readTree("{ \"-1\": \"totally allowed\" }");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path/-1",
            200);
    assertThat(objectMapper.readTree(resp))
        .isEqualTo(wrapResponse(objectMapper.readTree("\"totally allowed\""), "1", null));

    obj = objectMapper.readTree("{ \"Eric says \\\"hello\\\"\": \"totally allowed\" }");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/path",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));
  }

  @Test
  public void testInvalidDepthAndLength() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("tooDeep.json"));
    String resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            400);
    assertThat(resp).isEqualTo("Max depth of 64 exceeded");

    obj = objectMapper.readTree("{ \"some\": \"json\" }");
    resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/[1000000]",
            obj.toString(),
            400);
    assertThat(resp).isEqualTo("Max array length of 1000000 exceeded.");
  }

  @Test
  public void testArrayGet() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            200);
    assertThat(resp).isEqualTo("{\"documentId\":\"1\"}");

    resp =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/1/quiz/maths/q1/options/[0]",
            200);
    assertThat(objectMapper.readTree(resp))
        .isEqualTo(wrapResponse(obj.requiredAt("/quiz/maths/q1/options/0"), "1", null));

    resp =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/1/quiz/maths/q1/options/[0]?raw=true",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(obj.requiredAt("/quiz/maths/q1/options/0"));

    resp =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/1/quiz/nests/q1/options/[3]/this",
            200);
    assertThat(objectMapper.readTree(resp))
        .isEqualTo(wrapResponse(obj.requiredAt("/quiz/nests/q1/options/3/this"), "1", null));
  }

  @Test
  public void testInvalidPathGet() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String resp =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            200);
    assertThat(resp).isEqualTo("{\"documentId\":\"1\"}");

    RestUtils.get(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/nonexistent/path",
        204);

    RestUtils.get(
        authToken,
        hostWithPort
            + "/v2/namespaces/"
            + keyspace
            + "/collections/collection/1/nonexistent/path/[1]",
        204);

    RestUtils.get(
        authToken,
        hostWithPort
            + "/v2/namespaces/"
            + keyspace
            + "/collections/collection/1/quiz/maths/q1/options/[9999]",
        204); // out of bounds
  }

  @Test
  public void testPutNullsAndEmpties() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": null}");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    String resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("{\"abc\": {}}");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/2",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/2",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "2", null));

    obj = objectMapper.readTree("{\"abc\": []}");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/3",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/3",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "3", null));

    obj =
        objectMapper.readTree(
            "{\"abc\": [], \"bcd\": {}, \"cde\": null, \"abcd\": { \"nest1\": [], \"nest2\": {}}}");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/4",
        obj.toString(),
        200);

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/4",
            200);
    assertThat(objectMapper.readTree(resp)).isEqualTo(wrapResponse(obj, "4", null));

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/4/abc",
            200);
    assertThat(objectMapper.readTree(resp))
        .isEqualTo(wrapResponse(objectMapper.createArrayNode(), "4", null));

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/4/bcd",
            200);
    assertThat(objectMapper.readTree(resp))
        .isEqualTo(wrapResponse(objectMapper.createObjectNode(), "4", null));

    resp =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/4/abcd/nest1",
            200);
    assertThat(objectMapper.readTree(resp))
        .isEqualTo(wrapResponse(objectMapper.createArrayNode(), "4", null));
  }

  @Test
  public void testPutReplacingObject() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            fullObj.toString(),
            200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    JsonNode obj;
    obj = objectMapper.readTree("{\"q5000\": \"hello?\"}");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);

    ObjectNode sportNode = objectMapper.createObjectNode();
    sportNode.set("q5000", TextNode.valueOf("hello?"));

    ObjectNode fullObjNode = (ObjectNode) fullObj;
    ((ObjectNode) fullObjNode.get("quiz")).set("sport", sportNode);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(fullObjNode, "1", null));
  }

  @Test
  public void testPutReplacingArrayElement() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            fullObj.toString(),
            200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    JsonNode obj;
    obj = objectMapper.readTree("{\"q5000\": \"hello?\"}");
    RestUtils.put(
        authToken,
        hostWithPort
            + "/v2/namespaces/"
            + keyspace
            + "/collections/collection/1/quiz/nests/q1/options/[0]",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/1/quiz/nests/q1/options/[0]",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);

    ObjectNode optionNode = objectMapper.createObjectNode();
    optionNode.set("q5000", TextNode.valueOf("hello?"));

    ObjectNode fullObjNode = (ObjectNode) fullObj;
    ((ArrayNode) fullObjNode.at("/quiz/nests/q1/options")).set(0, optionNode);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(fullObjNode, "1", null));
  }

  @Test
  public void testPutReplacingWithArray() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            fullObj.toString(),
            200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    JsonNode obj = objectMapper.readTree("[{\"array\": \"at\"}, \"sub\", \"doc\"]");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("[0, \"a\", \"2\", true]");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/nests/q1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/nests/q1",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    ObjectNode nestsNode =
        (ObjectNode) objectMapper.readTree("{\"nests\":{\"q1\":[0,\"a\",\"2\",true]}}");
    ObjectNode fullObjNode = (ObjectNode) fullObj;
    fullObjNode.set("quiz", nestsNode);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(fullObjNode, "1", null));

    obj = objectMapper.readTree("[{\"array\": \"at\"}, \"\", \"doc\"]");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("{\"we\": {\"are\": \"done\"}}");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));
  }

  @Test
  public void testInvalidPuts() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            fullObj.toString(),
            200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    JsonNode obj;
    obj = objectMapper.readTree("3");
    r =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            400);
    assertThat(r)
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: 3\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("true");
    r =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz",
            obj.toString(),
            400);
    assertThat(r)
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: true\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("null");
    r =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz",
            obj.toString(),
            400);
    assertThat(r)
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: null\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("\"Eric\"");
    r =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport",
            obj.toString(),
            400);
    assertThat(r)
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: \"Eric\"\nHint: update the parent path with a defined object instead.");
  }

  @Test
  public void testDelete() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            fullObj.toString(),
            200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    RestUtils.delete(
        authToken,
        hostWithPort
            + "/v2/namespaces/"
            + keyspace
            + "/collections/collection/1/quiz/sport/q1/question",
        204);

    RestUtils.get(
        authToken,
        hostWithPort
            + "/v2/namespaces/"
            + keyspace
            + "/collections/collection/1/quiz/sport/q1/question",
        204);

    RestUtils.delete(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/maths",
        204);

    RestUtils.get(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/maths",
        204);

    RestUtils.delete(
        authToken,
        hostWithPort
            + "/v2/namespaces/"
            + keyspace
            + "/collections/collection/1/quiz/nests/q1/options/[0]",
        204);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/1/quiz/nests/q1/options",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree(
                    "[null,\"not a nest\",\"definitely not a nest\",{ \"this\":  true }]"),
                "1",
                null));

    RestUtils.delete(
        authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1", 204);

    RestUtils.get(
        authToken, hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1", 204);
  }

  @Test
  public void testPost() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response resp =
        RestUtils.postRaw(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection",
            fullObj.toString(),
            201);
    String newLocation = resp.header("location");
    String body = resp.body().string();
    String newId = objectMapper.readTree(body).requiredAt("/documentId").asText();
    assertThat(newId).isNotNull();

    String r = RestUtils.get(authToken, newLocation, 200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(fullObj, newId, null));
  }

  @Test
  public void testRootDocumentPatch() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": 1}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    String r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("{\"bcd\": true}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree("{ \"abc\": 1, \"bcd\": true }"), "1", null));

    obj = objectMapper.readTree("{\"bcd\": {\"a\": {\"b\": 0 }}}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": 1, \"bcd\": {\"a\": {\"b\": 0 }} }"), "1", null));

    obj = objectMapper.readTree("{\"bcd\": [1,2,3,4]}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(objectMapper.readTree("{ \"abc\": 1, \"bcd\": [1,2,3,4] }"), "1", null));

    obj = objectMapper.readTree("{\"bcd\": [5,{\"a\": 23},7,8]}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": 1, \"bcd\": [5,{\"a\": 23},7,8] }"), "1", null));

    obj = objectMapper.readTree("{\"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree(
                    "{ \"abc\": 1, \"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] }"),
                "1",
                null));

    obj = objectMapper.readTree("{\"bcd\": {\"replace\": \"array\"}}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"} }"),
                "1",
                null));

    obj = objectMapper.readTree("{\"done\": \"done\"}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree(
                    "{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"}, \"done\": \"done\" }"),
                "1",
                null));
  }

  @Test
  public void testRootDocumentPatchNulls() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": null}");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    String r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(obj, "1", null));

    obj = objectMapper.readTree("{\"bcd\": null}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(objectMapper.readTree("{ \"abc\": null, \"bcd\": null }"), "1", null));

    obj = objectMapper.readTree("{\"bcd\": {\"a\": {\"b\": null }}}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": null, \"bcd\": {\"a\": {\"b\": null }} }"),
                "1",
                null));

    obj = objectMapper.readTree("{\"bcd\": [null,2,null,4]}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": null, \"bcd\": [null,2,null,4] }"), "1", null));

    obj = objectMapper.readTree("{\"bcd\": [1,{\"a\": null},3,4]}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": null, \"bcd\": [1,{\"a\": null},3,4] }"),
                "1",
                null));

    obj = objectMapper.readTree("{\"bcd\": [null]}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(objectMapper.readTree("{ \"abc\": null, \"bcd\": [null] }"), "1", null));

    obj = objectMapper.readTree("{\"null\": null}");
    RestUtils.patch(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        obj.toString(),
        200);

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": null, \"bcd\": [null], \"null\": null }"),
                "1",
                null));
  }

  @Test
  public void testInvalidPatches() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    String r =
        RestUtils.put(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            fullObj.toString(),
            200);
    assertThat(r).isEqualTo("{\"documentId\":\"1\"}");

    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            200);
    assertThat(objectMapper.readTree(r)).isEqualTo(wrapResponse(fullObj, "1", null));

    JsonNode obj;
    obj = objectMapper.readTree("[{\"array\": \"at\"}, \"root\", \"doc\"]");
    r =
        RestUtils.patch(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            400);
    assertThat(r).isEqualTo("A patch operation must be done with a JSON object, not an array.");

    // For patching, you must use an object, so arrays even patched to sub-paths are not allowed.
    r =
        RestUtils.patch(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport/q1",
            obj.toString(),
            400);
    assertThat(r).isEqualTo("A patch operation must be done with a JSON object, not an array.");

    obj = objectMapper.readTree("3");
    r =
        RestUtils.patch(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
            obj.toString(),
            400);
    assertThat(r)
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: 3\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("true");
    r =
        RestUtils.patch(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz",
            obj.toString(),
            400);
    assertThat(r)
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: true\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("null");
    r =
        RestUtils.patch(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz",
            obj.toString(),
            400);
    assertThat(r)
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: null\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("\"Eric\"");
    r =
        RestUtils.patch(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport",
            obj.toString(),
            400);
    assertThat(r)
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: \"Eric\"\nHint: update the parent path with a defined object instead.");
  }

  @Test
  public void testBasicSearch() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id",
        fullObj.toString(),
        200);

    // EQ
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.Pixel_3a.price\": {\"$eq\": 600}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    RestUtils.get(
        authToken,
        hostWithPort
            + "/v2/namespaces/"
            + keyspace
            + "/collections/collection/cool-search-id?where={\"price\": {\"$eq\": 600}}&raw=true",
        204);

    // LT
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.*.price\": {\"$lt\": 600}}&raw=true",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));

    // LTE
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.*.price\": {\"$lte\": 600}}",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // GT
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.price\": {\"$gt\": 600}}&raw=true",
            200);

    searchResultStr = "[{\"products\": {\"electronics\": {\"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));

    // GTE
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.price\": {\"$gte\": 600}}&raw=true",
            200);

    searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));

    // EXISTS
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$exists\": true}}&raw=true",
            200);
    searchResultStr =
        "["
            + "{\"products\":{\"electronics\":{\"Pixel_3a\":{\"price\":600}}}},"
            + "{\"products\":{\"electronics\":{\"iPhone_11\":{\"price\":900}}}},"
            + "{\"products\":{\"food\":{\"Apple\":{\"price\":0.99}}}},"
            + "{\"products\":{\"food\":{\"Pear\":{\"price\":0.89}}}}"
            + "]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));
  }

  @Test
  public void testBasicSearchSelectionSet() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id",
        fullObj.toString(),
        200);

    // EQ
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.Pixel_3a.price\": {\"$eq\": 600}}&fields=[\"name\", \"price\", \"model\", \"manufacturer\"]",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"name\": \"Pixel\", \"manufacturer\": \"Google\", \"model\": \"3a\", \"price\": 600}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // LT
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.*.price\": {\"$lt\": 600}}&fields=[\"name\", \"price\", \"model\"]&raw=true",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"name\": \"apple\", \"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"name\": \"pear\", \"price\": 0.89}}}}]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));

    // LTE
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.*.price\": {\"$lte\": 600}}&fields=[\"price\", \"sku\"]",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99, \"sku\": \"100100010101001\"}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89, \"sku\": null}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // GT
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.price\": {\"$gt\": 600}}&fields=[\"price\", \"throwaway\"]&raw=true",
            200);

    searchResultStr = "[{\"products\": {\"electronics\": {\"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));

    // GTE
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.price\": {\"$gte\": 600}}&fields=[\"price\"]&raw=true",
            200);

    searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));

    // EXISTS
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$exists\": true}}&fields=[\"price\", \"name\", \"manufacturer\", \"model\", \"sku\"]&raw=true",
            200);
    searchResultStr =
        "["
            + "{\"products\":{\"electronics\":{\"Pixel_3a\":{\"price\":600, \"name\":\"Pixel\", \"manufacturer\":\"Google\", \"model\":\"3a\"}}}},"
            + "{\"products\":{\"electronics\":{\"iPhone_11\":{\"price\":900, \"name\":\"iPhone\", \"manufacturer\":\"Apple\", \"model\":\"11\"}}}},"
            + "{\"products\":{\"food\":{\"Apple\":{\"name\": \"apple\", \"price\":0.99, \"sku\": \"100100010101001\"}}}},"
            + "{\"products\":{\"food\":{\"Pear\":{\"name\": \"pear\", \"price\":0.89, \"sku\": null}}}}"
            + "]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));
  }

  @Test
  public void testSearchNotEquals() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id",
        fullObj.toString(),
        200);

    // NE with String
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.model\": {\"$ne\": \"3a\"}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NE with Boolean
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.nests.q1.options.[3].this\": {\"$ne\": false}}",
            200);

    searchResultStr =
        "[{\"quiz\": {\"nests\": { \"q1\": {\"options\": {\"[3]\": {\"this\": true}}}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NE with integer compared to double
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.maths.q1.answer\": {\"$ne\": 12}}",
            200);

    searchResultStr = "[{\"quiz\": {\"maths\": { \"q1\": {\"answer\": 12.2}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NE with double compared to integer
    RestUtils.get(
        authToken,
        hostWithPort
            + "/v2/namespaces/"
            + keyspace
            + "/collections/collection/cool-search-id?where={\"quiz.maths.q2.answer\": {\"$ne\": 4.0}}",
        204);

    // NE with null
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.*.sku\": {\"$ne\": null}}",
            200);

    searchResultStr = "[{\"products\": {\"food\": { \"Apple\": {\"sku\": \"100100010101001\"}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testFullCollectionSearchFilterNesting() throws IOException {
    JsonNode fullObj1 =
        objectMapper.readTree("{\"someStuff\": {\"someOtherStuff\": {\"value\": \"a\"}}}");
    JsonNode fullObj2 = objectMapper.readTree("{\"value\": \"a\"}");
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id",
        fullObj1.toString(),
        200);
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id-2",
        fullObj2.toString(),
        200);

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

    String expected = "{\"cool-search-id-2\":{\"value\":\"a\"}}";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(expected));

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"someStuff.someOtherStuff.value\": {\"$eq\": \"a\"}}&raw=true",
            200);

    expected = "{\"cool-search-id\":{\"someStuff\": {\"someOtherStuff\": {\"value\": \"a\"}}}}";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(expected));

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&where={\"someStuff.*.value\": {\"$eq\": \"a\"}}&raw=true",
            200);

    expected = "{\"cool-search-id\":{\"someStuff\": {\"someOtherStuff\": {\"value\": \"a\"}}}}";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(expected));
  }

  @Test
  public void testSearchIn() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id",
        fullObj.toString(),
        200);

    // IN with String
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.model\": {\"$in\": [\"11\", \"3a\"]}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // IN with int
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$in\": [600, 900]}}",
            200);

    searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // IN with double
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$in\": [0.99, 0.89]}}",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": { \"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // IN with null
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.sku\": {\"$in\": [null]}}",
            200);

    searchResultStr = "[{\"products\": {\"food\": { \"Pear\": {\"sku\": null}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testSearchNotIn() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id",
        fullObj.toString(),
        200);

    // NIN with String
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.model\": {\"$nin\": [\"12\"]}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NIN with int
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$nin\": [600, 900]}}",
            200);

    searchResultStr =
        "[{\"products\": {\"food\": { \"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NIN with double
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$nin\": [0.99, 0.89]}}",
            200);

    searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NIN with null
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.sku\": {\"$nin\": [null]}}",
            200);

    searchResultStr = "[{\"products\": {\"food\": { \"Apple\": {\"sku\": \"100100010101001\"}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testFilterCombos() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id",
        fullObj.toString(),
        200);

    // NIN (limited support) with GT (full support)
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.model\": {\"$nin\": [\"11\"], \"$gt\": \"\"}}",
            200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // IN (limited support) with NE (limited support)
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.model\": {\"$in\": [\"11\", \"3a\"], \"$ne\": \"11\"}}",
            200);

    searchResultStr = "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}]";
    assertThat(objectMapper.readTree(r))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testInvalidSearch() throws IOException {
    RestUtils.get(
        authToken,
        hostWithPort
            + "/v2/namespaces/"
            + keyspace
            + "/collections/collection/cool-search-id?where=hello",
        500);

    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where=[\"a\"]}",
            400);
    assertThat(r).isEqualTo("Search was expecting a JSON object as input.");

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": true}}",
            400);
    assertThat(r).isEqualTo("Search entry for field a was expecting a JSON object as input.");

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$exists\": false}}}",
            400);
    assertThat(r).isEqualTo("$exists only supports the value `true`");

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"exists\": true}}}",
            400);
    assertThat(r).startsWith("Invalid operator: exists, valid operators are:");

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$eq\": null}}}",
            400);
    assertThat(r)
        .isEqualTo("Value entry for field a, operation $eq was expecting a non-null value");

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$eq\": {}}}}",
            400);
    assertThat(r)
        .isEqualTo("Value entry for field a, operation $eq was expecting a non-null value");

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$eq\": []}}}",
            400);
    assertThat(r)
        .isEqualTo("Value entry for field a, operation $eq was expecting a non-null value");

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$in\": 2}}}",
            400);
    assertThat(r).isEqualTo("Value entry for field a, operation $in was expecting an array");

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$eq\": 300}, \"b\": {\"$lt\": 500}}",
            400);
    assertThat(r).contains("Conditions across multiple fields are not yet supported");

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$in\": [1]}}&fields=[\"b\"]",
            400);
    assertThat(r)
        .contains(
            "When selecting `fields`, the field referenced by `where` must be in the selection.");

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?fields=[\"b\"]",
            400);
    assertThat(r).contains("Selecting fields is not allowed without `where`");
  }

  @Test
  public void testMultiSearch() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id",
        fullObj.toString(),
        200);

    // Multiple operators
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.Orange.info.price\": {\"$gt\": 600, \"$lt\": 600.05}}&raw=true",
            200);

    String searchResultStr =
        "[{\"products\": {\"food\": {\"Orange\": {\"info\": {\"price\": 600.01}}}}}]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));

    // Array paths
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.maths.q1.options.[0]\": {\"$lt\": 13.3}}&raw=true",
            200);
    searchResultStr = "[{\"quiz\":{\"maths\":{\"q1\":{\"options\":{\"[0]\":10.2}}}}}]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.nests.q2.options.*.this.that.them\": {\"$eq\": false}}&raw=true",
            200);
    searchResultStr =
        "[{\"quiz\":{\"nests\":{\"q2\":{\"options\":{\"[3]\":{\"this\":{\"that\":{\"them\":false}}}}}}}}]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));

    // Multi-path
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.nests.q1,q2.options.[0]\": {\"$eq\": \"nest\"}}&raw=true",
            200);
    searchResultStr =
        "["
            + "{\"quiz\":{\"nests\":{\"q1\":{\"options\":{\"[0]\":\"nest\"}}}}},"
            + "{\"quiz\":{\"nests\":{\"q2\":{\"options\":{\"[0]\":\"nest\"}}}}}"
            + "]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));

    // Multi-path...and glob?!?
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.nests.q2,q3.options.*.this.them\": {\"$eq\": false}}&raw=true",
            200);
    searchResultStr =
        "[{\"quiz\":{\"nests\":{\"q3\":{\"options\":{\"[2]\":{\"this\":{\"them\":false}}}}}}}]";
    assertThat(objectMapper.readTree(r)).isEqualTo(objectMapper.readTree(searchResultStr));
  }

  @Test
  public void testPaginationSingleDocSearch() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id",
        fullObj.toString(),
        200);

    // With default page size
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"*.value\": {\"$gt\": 0}}",
            200);
    JsonNode responseBody1 = objectMapper.readTree(r);

    assertThat(responseBody1.requiredAt("/data").size()).isEqualTo(100);
    String pageState = responseBody1.requiredAt("/pageState").requireNonNull().asText();

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"*.value\": {\"$gt\": 0}}&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    JsonNode responseBody2 = objectMapper.readTree(r);

    assertThat(responseBody2.requiredAt("/data").size()).isEqualTo(5);
    assertThat(responseBody2.at("/pageState").isMissingNode()).isTrue();

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
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"*.value\": {\"$gt\": 1}}&page-size=50",
            200);
    responseBody1 = objectMapper.readTree(r);

    assertThat(responseBody1.requiredAt("/data").size()).isEqualTo(50);
    pageState = responseBody1.requiredAt("/pageState").requireNonNull().asText();

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"*.value\": {\"$gt\": 1}}&page-size=50&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    responseBody2 = objectMapper.readTree(r);

    assertThat(responseBody2.requiredAt("/data").size()).isEqualTo(34);
    assertThat(responseBody2.at("/pageState").isMissingNode()).isTrue();

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
  public void testGetFullDocMultiFilter() throws IOException {
    JsonNode doc = objectMapper.readTree("{\"a\": \"b\", \"c\": 2}");

    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        doc.toString(),
        200);

    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"a\":{\"$eq\":\"b\"},\"c\":{\"$lt\":3}}",
            200);

    JsonNode resp = objectMapper.readTree(r);
    JsonNode data = resp.requiredAt("/data");

    ObjectNode expected = objectMapper.createObjectNode();
    expected.set("a", new TextNode("b"));
    expected.set("c", new IntNode(2));
    assertThat(data.requiredAt("/1")).isEqualTo(expected);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"a\":{\"$eq\":\"b\"},\"c\":{\"$lt\":0}}",
            200);

    // resulting JSON should be empty
    resp = objectMapper.readTree(r);
    data = resp.requiredAt("/data");

    expected = objectMapper.createObjectNode();
    assertThat(data).isEqualTo(expected);
  }

  @Test
  public void testPaginationGetFullDoc() throws IOException {
    JsonNode doc1 =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    JsonNode doc2 = objectMapper.readTree("{\"a\": \"b\"}");
    JsonNode doc3 =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));

    ObjectNode docsByKey = objectMapper.createObjectNode();
    docsByKey.set("1", doc1);
    docsByKey.set("2", doc2);
    docsByKey.set("3", doc3);

    Set<String> docsSeen = new HashSet<>();

    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        doc1.toString(),
        200);
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/2",
        doc2.toString(),
        200);
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/3",
        doc3.toString(),
        200);

    // page-size defaults to 1 document when excluded
    String r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection",
            200);
    JsonNode resp = objectMapper.readTree(r);
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
                + "/collections/collection?page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = objectMapper.readTree(r);
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
                + "/collections/collection?page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = objectMapper.readTree(r);
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
    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection?page-size=2",
            200);
    resp = objectMapper.readTree(r);
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
    resp = objectMapper.readTree(r);
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
    r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection?page-size=4",
            200);
    resp = objectMapper.readTree(r);
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
        objectMapper.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    JsonNode doc2 = objectMapper.readTree("{\"a\": \"b\"}");
    JsonNode doc3 =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));

    ObjectNode docsByKey = objectMapper.createObjectNode();
    docsByKey.set("1", doc1);
    docsByKey.set("2", doc2);
    docsByKey.set("3", doc3);

    Set<String> docsSeen = new HashSet<>();

    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        doc1.toString(),
        200);
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/2",
        doc2.toString(),
        200);
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/3",
        doc3.toString(),
        200);

    // page-size defaults to 1 document when excluded
    String r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection?fields=[\"a\"]",
            200);
    JsonNode resp = objectMapper.readTree(r);
    String pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    JsonNode data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    String key = data.fieldNames().next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(objectMapper.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(objectMapper.readTree("{}"));
    }

    docsSeen.add(key);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?fields=[\"a\"]&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = objectMapper.readTree(r);
    pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    key = data.fieldNames().next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(objectMapper.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(objectMapper.readTree("{}"));
    }
    docsSeen.add(key);

    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?fields=[\"a\"]&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = objectMapper.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    key = data.fieldNames().next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(objectMapper.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(objectMapper.readTree("{}"));
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
    resp = objectMapper.readTree(r);
    pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(2);
    // Any two documents could come back, find out which are there
    Iterator<String> fieldNames = data.fieldNames();
    key = fieldNames.next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(objectMapper.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(objectMapper.readTree("{}"));
    }
    docsSeen.add(key);
    key = fieldNames.next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(objectMapper.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(objectMapper.readTree("{}"));
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
    resp = objectMapper.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    key = data.fieldNames().next();
    if (key.equals("1")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(objectMapper.readTree("{\"a\": {\"value\": 1}}"));
    } else if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    } else {
      assertThat(data.requiredAt("/" + key)).isEqualTo(objectMapper.readTree("{}"));
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
    resp = objectMapper.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(3);
    Iterator<String> iter = data.fieldNames();
    while (iter.hasNext()) {
      String field = iter.next();
      docsSeen.add(field);
      JsonNode node = data.requiredAt("/" + field);
      if (field.equals("1")) {
        assertThat(node).isEqualTo(objectMapper.readTree("{\"a\": {\"value\": 1}}"));
      } else if (field.equals("2")) {
        assertThat(node).isEqualTo(docsByKey.requiredAt("/" + field));
      } else {
        assertThat(node).isEqualTo(objectMapper.readTree("{}"));
      }
    }

    // Make sure we've seen every key
    assertThat(docsSeen.size()).isEqualTo(3);
  }

  @Test
  public void testPaginationFilterDocWithFields() throws IOException {
    JsonNode doc1 =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    JsonNode doc2 =
        objectMapper.readTree(
            "{\"a\": \"b\", \"quiz\": {\"sport\": {\"q1\": {\"question\": \"hello?\"}}}}");
    JsonNode doc3 =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));

    ObjectNode docsByKey = objectMapper.createObjectNode();
    docsByKey.set("1", doc1);
    docsByKey.set("2", doc2);
    docsByKey.set("3", doc3);

    Set<String> docsSeen = new HashSet<>();

    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        doc1.toString(),
        200);
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/2",
        doc2.toString(),
        200);
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/3",
        doc3.toString(),
        200);

    // page-size defaults to 1 document when excluded
    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"b.value\": {\"$eq\": 2}}&fields=[\"a\"]",
            200);
    JsonNode resp = objectMapper.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    JsonNode data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // The only matching document based on `where` is document 1
    String key = data.fieldNames().next();
    assertThat(key).isEqualTo("1");
    assertThat(data.requiredAt("/" + key))
        .isEqualTo(objectMapper.readTree("{\"a\": {\"value\": 1}}"));

    // Return all documents where quiz.sport.q1.question exists, but only the `quiz` field on each
    // doc
    r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"quiz.sport.q1.question\": {\"$exists\": true}}&fields=[\"quiz\"]",
            200);
    resp = objectMapper.readTree(r);
    String pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // The document could either be document 2 or document 3
    key = data.fieldNames().next();
    if (key.equals("2")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(
              objectMapper.readTree(
                  "{\"quiz\": {\"sport\": {\"q1\": {\"question\": \"hello?\"}}}}"));
    } else {
      ObjectNode expected = objectMapper.createObjectNode();
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
                + "/collections/collection?where={\"quiz.sport.q1.question\": {\"$exists\": true}}&fields=[\"quiz\"]&page-state="
                + URLEncoder.encode(pageState, "UTF-8"),
            200);
    resp = objectMapper.readTree(r);
    assertThat(resp.at("/pageState").isMissingNode()).isTrue();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    key = data.fieldNames().next();
    if (docsSeen.contains("3")) {
      assertThat(data.requiredAt("/" + key))
          .isEqualTo(
              objectMapper.readTree(
                  "{\"quiz\": {\"sport\": {\"q1\": {\"question\": \"hello?\"}}}}"));
    } else if (docsSeen.contains("2")) {
      ObjectNode expected = objectMapper.createObjectNode();
      expected.set("quiz", doc3.requiredAt("/quiz"));
      assertThat(data.requiredAt("/" + key)).isEqualTo(expected);
    }
    docsSeen.add(key);
    assertThat(docsSeen.size()).isEqualTo(2);
    assertThat(docsSeen.containsAll(ImmutableList.of("2", "3"))).isTrue();
  }

  @Test
  public void testInvalidFullDocPageSize() throws IOException {
    String r =
        RestUtils.get(
            authToken,
            hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection?page-size=21",
            400);
    assertThat(r).isEqualTo("The parameter `page-size` is limited to 20.");
  }

  @Test
  public void testPaginationDisallowedLimitedSupport() throws IOException {
    JsonNode doc1 =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    RestUtils.put(
        authToken,
        hostWithPort + "/v2/namespaces/" + keyspace + "/collections/collection/1",
        doc1.toString(),
        200);

    String r =
        RestUtils.get(
            authToken,
            hostWithPort
                + "/v2/namespaces/"
                + keyspace
                + "/collections/collection/1?where={\"*.value\":{\"$nin\": [3]}}page-size=5",
            400);
    assertThat(r)
        .isEqualTo(
            "The results as requested must fit in one page, try increasing the `page-size` parameter.");
  }

  // Below are "namespace" tests that should match the REST Api's keyspace tests
  @Test
  public void getNamespaces() throws IOException {
    String body =
        RestUtils.get(
            authToken, String.format("%s:8082/v2/schemas/namespaces", host), HttpStatus.SC_OK);

    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    List<Keyspace> keyspaces =
        objectMapper.convertValue(response.getData(), new TypeReference<List<Keyspace>>() {});
    assertThat(keyspaces)
        .anySatisfy(
            value ->
                assertThat(value).isEqualToComparingFieldByField(new Keyspace("system", null)));
  }

  @Test
  public void getNamespacesMissingToken() throws IOException {
    RestUtils.get(
        "", String.format("%s:8082/v2/schemas/namespaces", host), HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void getNamespacesBadToken() throws IOException {
    RestUtils.get(
        "foo", String.format("%s:8082/v2/schemas/namespaces", host), HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void getNamespacesRaw() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/namespaces?raw=true", host),
            HttpStatus.SC_OK);

    List<Keyspace> keyspaces = objectMapper.readValue(body, new TypeReference<List<Keyspace>>() {});
    assertThat(keyspaces)
        .anySatisfy(
            value ->
                assertThat(value)
                    .isEqualToComparingFieldByField(new Keyspace("system_schema", null)));
  }

  @Test
  public void getNamespace() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/namespaces/system", host),
            HttpStatus.SC_OK);

    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    Keyspace keyspace = objectMapper.convertValue(response.getData(), Keyspace.class);

    assertThat(keyspace).isEqualToComparingFieldByField(new Keyspace("system", null));
  }

  @Test
  public void getNamespaceRaw() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/namespaces/system?raw=true", host),
            HttpStatus.SC_OK);

    Keyspace keyspace = objectMapper.readValue(body, Keyspace.class);

    assertThat(keyspace).isEqualToComparingFieldByField(new Keyspace("system", null));
  }

  @Test
  public void getNamespaceNotFound() throws IOException {
    RestUtils.get(
        authToken,
        String.format("%s:8082/v2/schemas/namespaces/ks_not_found", host),
        HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void createNamespace() throws IOException {
    String keyspaceName = "ks_createkeyspace_" + System.currentTimeMillis();
    createKeyspace(keyspaceName);

    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/namespaces/%s?raw=true", host, keyspaceName),
            HttpStatus.SC_OK);

    Keyspace keyspace = objectMapper.readValue(body, Keyspace.class);

    assertThat(keyspace).isEqualToComparingFieldByField(new Keyspace(keyspaceName, null));
  }

  @Test
  public void deleteNamespace() throws IOException {
    String keyspaceName = "ks_createkeyspace_" + System.currentTimeMillis();
    createKeyspace(keyspaceName);

    RestUtils.get(
        authToken,
        String.format("%s:8082/v2/schemas/namespaces/%s", host, keyspaceName),
        HttpStatus.SC_OK);

    RestUtils.delete(
        authToken,
        String.format("%s:8082/v2/schemas/namespaces/%s", host, keyspaceName),
        HttpStatus.SC_NO_CONTENT);

    RestUtils.get(
        authToken,
        String.format("%s:8082/v2/schemas/namespaces/%s", host, keyspaceName),
        HttpStatus.SC_NOT_FOUND);
  }

  private JsonNode wrapResponse(JsonNode node, String id, String pagingState) {
    ObjectNode wrapperNode = objectMapper.createObjectNode();

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

  private void createKeyspace(String keyspaceName) throws IOException {
    String createKeyspaceRequest =
        String.format("{\"name\": \"%s\", \"replicas\": 1}", keyspaceName);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/schemas/namespaces", host),
        createKeyspaceRequest,
        HttpStatus.SC_CREATED);
  }
}
