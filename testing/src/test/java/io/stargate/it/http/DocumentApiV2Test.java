package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class DocumentApiV2Test extends BaseOsgiIntegrationTest {
  private static Logger logger = LoggerFactory.getLogger(DocumentApiV2Test.class);

  private String keyspace;

  private static String authToken;
  private static String host = "http://" + getStargateHost();
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final OkHttpClient client = new OkHttpClient().newBuilder().build();

  public DocumentApiV2Test(ClusterConnectionInfo backend) {
    super(backend);
  }

  @BeforeEach
  public void setup(ClusterConnectionInfo cluster) throws IOException {
    keyspace = "ks_docs_" + System.currentTimeMillis();

    CqlSession session =
        CqlSession.builder()
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(1))
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(20))
                    .build())
            .withAuthCredentials("cassandra", "cassandra")
            .addContactPoint(new InetSocketAddress(getStargateHost(), 9043))
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
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/maths");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(obj.requiredAt("/quiz/maths"), "1"));

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"products.electronics.Pixel_3a.price\":{\"$lt\": 800}}");
    assertThat(r.code()).isEqualTo(200);
    ObjectNode expected = objectMapper.createObjectNode();
    expected.set("1", obj);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(expected, null, null));
  }

  @Test
  public void testInvalidKeyspaceAndTable() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r = put("/v2/namespaces/unknown_keyspace_1337/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Unknown namespace unknown_keyspace_1337, you must create it first by creating a keyspace with the same name.");

    r = put("/v2/namespaces/" + keyspace + "/collections/invalid-character/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Could not create collection invalid-character, it has invalid characters. Valid characters are alphanumeric and underscores.");
  }

  @Test
  public void testInvalidKeyPut() throws IOException {
    JsonNode obj = objectMapper.readTree("{ \"square[]braces\": \"are not allowed\" }");

    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field square[]braces");

    obj = objectMapper.readTree("{ \"commas,\": \"are not allowed\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field commas,");

    obj = objectMapper.readTree("{ \"periods.\": \"are not allowed\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field periods.");

    obj = objectMapper.readTree("{ \"'quotes'\": \"are not allowed\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field 'quotes'");

    obj = objectMapper.readTree("{ \"*asterisks*\": \"are not allowed\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field *asterisks*");
  }

  @Test
  public void testWeirdButAllowedKeys() throws IOException {
    JsonNode obj = objectMapper.readTree("{ \"$\": \"weird but allowed\" }");
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/path", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{ \"$30\": \"not as weird\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/path", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{ \"@\": \"weird but allowed\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/path", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{ \"meet me @ the place\": \"not as weird\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/path", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{ \"?\": \"weird but allowed\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/path", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{ \"spac es\": \"weird but allowed\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/path", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{ \"3\": [\"totally allowed\"] }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/path", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path/3/[0]");
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree("\"totally allowed\""), "1"));

    obj = objectMapper.readTree("{ \"-1\": \"totally allowed\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/path", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path/-1");
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree("\"totally allowed\""), "1"));

    obj = objectMapper.readTree("{ \"Eric says \\\"hello\\\"\": \"totally allowed\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/path", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/path");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));
  }

  @Test
  public void testInvalidDepthAndLength() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("tooDeep.json"));
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).isEqualTo("Max depth of 64 exceeded");

    obj = objectMapper.readTree("{ \"some\": \"json\" }");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/[1000000]", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).isEqualTo("Max array length of 1000000 exceeded.");
  }

  @Test
  public void testArrayGet() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.body().string()).isEqualTo("{\"documentId\":\"1\"}");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/maths/q1/options/[0]");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(obj.requiredAt("/quiz/maths/q1/options/0"), "1"));

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/1/quiz/maths/q1/options/[0]?raw=true");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(obj.requiredAt("/quiz/maths/q1/options/0"));

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/1/quiz/nests/q1/options/[3]/this");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(obj.requiredAt("/quiz/nests/q1/options/3/this"), "1"));
  }

  @Test
  public void testInvalidPathGet() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.body().string()).isEqualTo("{\"documentId\":\"1\"}");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/nonexistent/path");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/nonexistent/path/[1]");
    assertThat(r.code()).isEqualTo(204);

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/1/quiz/maths/q1/options/[9999]"); // out of bounds
    assertThat(r.code()).isEqualTo(204);
  }

  @Test
  public void testRootDocumentPut() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": 1}");
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{\"bcd\": true}");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(409);
    assertThat(r.body().string()).isEqualTo("Document 1 already exists in collection collection");

    obj = objectMapper.readTree("{\"bcd\": true}");
    r = post("/v2/namespaces/" + keyspace + "/collections/collection", obj);
    assertThat(r.code()).isEqualTo(201);
    String body = r.body().string();
    JsonNode newId = objectMapper.readTree(body).requiredAt("/documentId");
    assertThat(newId).isNotNull();

    obj = objectMapper.readTree("{\"bcd\": true}");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/" + newId.asText(), obj);
    assertThat(r.code()).isEqualTo(409);
    assertThat(r.body().string())
        .isEqualTo(
            String.format("Document %s already exists in collection collection", newId.asText()));

    obj = objectMapper.readTree("{\"cde\": 1}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/2", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    obj = objectMapper.readTree("{\"bcd\": true}");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/2", obj);
    assertThat(r.code()).isEqualTo(409);
    assertThat(r.body().string()).isEqualTo("Document 2 already exists in collection collection");

    r = delete("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(204);

    obj = objectMapper.readTree("{\"bcd\": true}");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));
  }

  @Test
  public void testPutNullsAndEmpties() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": null}");
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{\"abc\": {}}");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/2", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/2");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "2"));

    obj = objectMapper.readTree("{\"abc\": []}");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/3", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/3");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "3"));

    obj =
        objectMapper.readTree(
            "{\"abc\": [], \"bcd\": {}, \"cde\": null, \"abcd\": { \"nest1\": [], \"nest2\": {}}}");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/4", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/4");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "4"));

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/4/abc");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.createArrayNode(), "4"));

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/4/bcd");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.createObjectNode(), "4"));

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/4/abcd/nest1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.createArrayNode(), "4"));
  }

  @Test
  public void testPutReplacingObject() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("{\"documentId\":\"1\"}");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(fullObj, "1"));

    JsonNode obj;
    obj = objectMapper.readTree("{\"q5000\": \"hello?\"}");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);

    ObjectNode sportNode = objectMapper.createObjectNode();
    sportNode.set("q5000", TextNode.valueOf("hello?"));

    ObjectNode fullObjNode = (ObjectNode) fullObj;
    ((ObjectNode) fullObjNode.get("quiz")).set("sport", sportNode);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(fullObjNode, "1"));
  }

  @Test
  public void testPutReplacingArrayElement() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("{\"documentId\":\"1\"}");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(fullObj, "1"));

    JsonNode obj;
    obj = objectMapper.readTree("{\"q5000\": \"hello?\"}");
    r =
        put(
            "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/nests/q1/options/[0]",
            obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/nests/q1/options/[0]");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);

    ObjectNode optionNode = objectMapper.createObjectNode();
    optionNode.set("q5000", TextNode.valueOf("hello?"));

    ObjectNode fullObjNode = (ObjectNode) fullObj;
    ((ArrayNode) fullObjNode.at("/quiz/nests/q1/options")).set(0, optionNode);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(fullObjNode, "1"));
  }

  @Test
  public void testPutReplacingWithArray() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("{\"documentId\":\"1\"}");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(fullObj, "1"));

    JsonNode obj = objectMapper.readTree("[{\"array\": \"at\"}, \"sub\", \"doc\"]");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("[0, \"a\", \"2\", true]");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/nests/q1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/nests/q1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    ObjectNode nestsNode =
        (ObjectNode) objectMapper.readTree("{\"nests\":{\"q1\":[0,\"a\",\"2\",true]}}");
    ObjectNode fullObjNode = (ObjectNode) fullObj;
    fullObjNode.set("quiz", nestsNode);

    String body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(objectMapper.readTree(body)).isEqualTo(wrapResponse(fullObjNode, "1"));
    assertThat(r.code()).isEqualTo(200);

    obj = objectMapper.readTree("[{\"array\": \"at\"}, \"\", \"doc\"]");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{\"we\": {\"are\": \"done\"}}");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));
  }

  @Test
  public void testInvalidPuts() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("{\"documentId\":\"1\"}");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(fullObj, "1"));

    JsonNode obj;
    obj = objectMapper.readTree("3");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: 3\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("true");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: true\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("null");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: null\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("\"Eric\"");
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: \"Eric\"\nHint: update the parent path with a defined object instead.");
  }

  @Test
  public void testDelete() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("{\"documentId\":\"1\"}");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(fullObj, "1"));

    r = delete("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport/q1/question");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport/q1/question");
    assertThat(r.code()).isEqualTo(204);

    r = delete("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/maths");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/maths");
    assertThat(r.code()).isEqualTo(204);

    r =
        delete(
            "/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/nests/q1/options/[0]");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/nests/q1/options");
    assertThat(r.code()).isEqualTo(200);
    String responseBody = r.body().string();
    assertThat(objectMapper.readTree(responseBody))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree(
                    "[null,\"not a nest\",\"definitely not a nest\",{ \"this\":  true }]"),
                "1"));

    r = delete("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(204);
  }

  @Test
  public void testPost() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response resp = post("/v2/namespaces/" + keyspace + "/collections/collection", fullObj);
    assertThat(resp.code()).isEqualTo(201);

    String newLocation = resp.header("location");
    String body = resp.body().string();
    String newId = objectMapper.readTree(body).requiredAt("/documentId").asText();
    assertThat(newId).isNotNull();

    resp = get(newLocation.replace(host + ":8082", ""));
    assertThat(resp.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(resp.body().string())).isEqualTo(wrapResponse(fullObj, newId));
  }

  @Test
  public void testRootDocumentPatch() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": 1}");
    Response r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{\"bcd\": true}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree("{ \"abc\": 1, \"bcd\": true }"), "1"));

    obj = objectMapper.readTree("{\"bcd\": {\"a\": {\"b\": 0 }}}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": 1, \"bcd\": {\"a\": {\"b\": 0 }} }"), "1"));

    obj = objectMapper.readTree("{\"bcd\": [1,2,3,4]}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree("{ \"abc\": 1, \"bcd\": [1,2,3,4] }"), "1"));

    obj = objectMapper.readTree("{\"bcd\": [5,{\"a\": 23},7,8]}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": 1, \"bcd\": [5,{\"a\": 23},7,8] }"), "1"));

    obj = objectMapper.readTree("{\"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree(
                    "{ \"abc\": 1, \"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] }"),
                "1"));

    obj = objectMapper.readTree("{\"bcd\": {\"replace\": \"array\"}}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"} }"), "1"));

    obj = objectMapper.readTree("{\"done\": \"done\"}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree(
                    "{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"}, \"done\": \"done\" }"),
                "1"));
  }

  @Test
  public void testRootDocumentPatchNulls() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": null}");
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(obj, "1"));

    obj = objectMapper.readTree("{\"bcd\": null}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree("{ \"abc\": null, \"bcd\": null }"), "1"));

    obj = objectMapper.readTree("{\"bcd\": {\"a\": {\"b\": null }}}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": null, \"bcd\": {\"a\": {\"b\": null }} }"), "1"));

    obj = objectMapper.readTree("{\"bcd\": [null,2,null,4]}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": null, \"bcd\": [null,2,null,4] }"), "1"));

    obj = objectMapper.readTree("{\"bcd\": [1,{\"a\": null},3,4]}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": null, \"bcd\": [1,{\"a\": null},3,4] }"), "1"));

    obj = objectMapper.readTree("{\"bcd\": [null]}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree("{ \"abc\": null, \"bcd\": [null] }"), "1"));

    obj = objectMapper.readTree("{\"null\": null}");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            wrapResponse(
                objectMapper.readTree("{ \"abc\": null, \"bcd\": [null], \"null\": null }"), "1"));
  }

  @Test
  public void testInvalidPatches() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("{\"documentId\":\"1\"}");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(wrapResponse(fullObj, "1"));

    JsonNode obj;
    obj = objectMapper.readTree("[{\"array\": \"at\"}, \"root\", \"doc\"]");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo("A patch operation must be done with a JSON object, not an array.");

    // For patching, you must use an object, so arrays even patched to sub-paths are not allowed.
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport/q1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo("A patch operation must be done with a JSON object, not an array.");

    obj = objectMapper.readTree("3");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: 3\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("true");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: true\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("null");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: null\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("\"Eric\"");
    r = patch("/v2/namespaces/" + keyspace + "/collections/collection/1/quiz/sport", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: \"Eric\"\nHint: update the parent path with a defined object instead.");
  }

  @Test
  public void testBasicSearch() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r =
        put("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id", fullObj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // EQ
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.Pixel_3a.price\": {\"$eq\": 600}}");
    assertThat(r.code()).isEqualTo(200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"price\": {\"$eq\": 600}}&raw=true");
    assertThat(r.code()).isEqualTo(204);
    r.close();

    // LT
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.*.price\": {\"$lt\": 600}}&raw=true");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // LTE
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.*.price\": {\"$lte\": 600}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // GT
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.price\": {\"$gt\": 600}}&raw=true");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr = "[{\"products\": {\"electronics\": {\"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // GTE
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.price\": {\"$gte\": 600}}&raw=true");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // EXISTS
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$exists\": true}}&raw=true");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr =
        "["
            + "{\"products\":{\"electronics\":{\"Pixel_3a\":{\"price\":600}}}},"
            + "{\"products\":{\"electronics\":{\"iPhone_11\":{\"price\":900}}}},"
            + "{\"products\":{\"food\":{\"Apple\":{\"price\":0.99}}}},"
            + "{\"products\":{\"food\":{\"Pear\":{\"price\":0.89}}}}"
            + "]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));
  }

  @Test
  public void testBasicSearchSelectionSet() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r =
        put("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id", fullObj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // EQ
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.Pixel_3a.price\": {\"$eq\": 600}}&fields=[\"name\", \"price\", \"model\", \"manufacturer\"]");
    assertThat(r.code()).isEqualTo(200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"name\": \"Pixel\", \"manufacturer\": \"Google\", \"model\": \"3a\", \"price\": 600}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // LT
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.*.price\": {\"$lt\": 600}}&fields=[\"name\", \"price\", \"model\"]&raw=true");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"name\": \"apple\", \"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"name\": \"pear\", \"price\": 0.89}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // LTE
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.*.price\": {\"$lte\": 600}}&fields=[\"price\", \"sku\"]");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99, \"sku\": \"100100010101001\"}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89, \"sku\": null}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // GT
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.price\": {\"$gt\": 600}}&fields=[\"price\", \"throwaway\"]&raw=true");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr = "[{\"products\": {\"electronics\": {\"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // GTE
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.price\": {\"$gte\": 600}}&fields=[\"price\"]&raw=true");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // EXISTS
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$exists\": true}}&fields=[\"price\", \"name\", \"manufacturer\", \"model\", \"sku\"]&raw=true");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr =
        "["
            + "{\"products\":{\"electronics\":{\"Pixel_3a\":{\"price\":600, \"name\":\"Pixel\", \"manufacturer\":\"Google\", \"model\":\"3a\"}}}},"
            + "{\"products\":{\"electronics\":{\"iPhone_11\":{\"price\":900, \"name\":\"iPhone\", \"manufacturer\":\"Apple\", \"model\":\"11\"}}}},"
            + "{\"products\":{\"food\":{\"Apple\":{\"name\": \"apple\", \"price\":0.99, \"sku\": \"100100010101001\"}}}},"
            + "{\"products\":{\"food\":{\"Pear\":{\"name\": \"pear\", \"price\":0.89, \"sku\": null}}}}"
            + "]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));
  }

  @Test
  public void testSearchNotEquals() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r =
        put("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id", fullObj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // NE with String
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.model\": {\"$ne\": \"3a\"}}");
    assertThat(r.code()).isEqualTo(200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NE with Boolean
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.nests.q1.options.[3].this\": {\"$ne\": false}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"quiz\": {\"nests\": { \"q1\": {\"options\": {\"[3]\": {\"this\": true}}}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NE with integer compared to double
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.maths.q1.answer\": {\"$ne\": 12}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr = "[{\"quiz\": {\"maths\": { \"q1\": {\"answer\": 12.2}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NE with double compared to integer
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.maths.q2.answer\": {\"$ne\": 4.0}}");
    assertThat(r.code()).isEqualTo(204);

    // NE with null
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.*.sku\": {\"$ne\": null}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr = "[{\"products\": {\"food\": { \"Apple\": {\"sku\": \"100100010101001\"}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testSearchIn() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r =
        put("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id", fullObj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // IN with String
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.model\": {\"$in\": [\"11\", \"3a\"]}}");
    assertThat(r.code()).isEqualTo(200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // IN with int
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$in\": [600, 900]}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // IN with double
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$in\": [0.99, 0.89]}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"products\": {\"food\": { \"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // IN with null
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.sku\": {\"$in\": [null]}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr = "[{\"products\": {\"food\": { \"Pear\": {\"sku\": null}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testSearchNotIn() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r =
        put("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id", fullObj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // NIN with String
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.model\": {\"$nin\": [\"12\"]}}");
    assertThat(r.code()).isEqualTo(200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NIN with int
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$nin\": [600, 900]}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"products\": {\"food\": { \"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NIN with double
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.price\": {\"$nin\": [0.99, 0.89]}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // NIN with null
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.*.*.sku\": {\"$nin\": [null]}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr = "[{\"products\": {\"food\": { \"Apple\": {\"sku\": \"100100010101001\"}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testFilterCombos() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r =
        put("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id", fullObj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // NIN (limited support) with GT (full support)
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.model\": {\"$nin\": [\"11\"], \"$gt\": \"\"}}");
    assertThat(r.code()).isEqualTo(200);

    String searchResultStr =
        "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));

    // IN (limited support) with NE (limited support)
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.electronics.*.model\": {\"$in\": [\"11\", \"3a\"], \"$ne\": \"11\"}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr = "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(wrapResponse(objectMapper.readTree(searchResultStr), "cool-search-id", null));
  }

  @Test
  public void testInvalidSearch() throws IOException {
    Response r =
        get("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id?where=hello");
    assertThat(r.code()).isEqualTo(500);
    r.close();

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id?where=[\"a\"]}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).isEqualTo("Search was expecting a JSON object as input.");

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": true}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo("Search entry for field a was expecting a JSON object as input.");

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$exists\": false}}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).isEqualTo("$exists only supports the value `true`");

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"exists\": true}}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).startsWith("Invalid operator: exists, valid operators are:");

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$eq\": null}}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo("Value entry for field a, operation $eq was expecting a non-null value");

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$eq\": {}}}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo("Value entry for field a, operation $eq was expecting a non-null value");

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$eq\": []}}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo("Value entry for field a, operation $eq was expecting a non-null value");

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$in\": 2}}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo("Value entry for field a, operation $in was expecting an array");

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$eq\": 300}, \"b\": {\"$lt\": 500}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .contains("Conditions across multiple fields are not yet supported");

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a.b\": {\"$eq\": 300}, \"c.b\": {\"$lt\": 500}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .contains("Conditions across multiple fields are not yet supported");

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"a\": {\"$in\": [1]}}&fields=[\"b\"]");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .contains(
            "When selecting `fields`, the field referenced by `where` must be in the selection.");

    r = get("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id?fields=[\"b\"]");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).contains("Selecting fields is not allowed without `where`");
  }

  @Test
  public void testMultiSearch() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("example.json"));
    Response r =
        put("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id", fullObj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // Multiple operators
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"products.food.Orange.info.price\": {\"$gt\": 600, \"$lt\": 600.05}}&raw=true");
    assertThat(r.code()).isEqualTo(200);

    String searchResultStr =
        "[{\"products\": {\"food\": {\"Orange\": {\"info\": {\"price\": 600.01}}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // Array paths
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.maths.q1.options.[0]\": {\"$lt\": 13.3}}&raw=true");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr = "[{\"quiz\":{\"maths\":{\"q1\":{\"options\":{\"[0]\":10.2}}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.nests.q2.options.*.this.that.them\": {\"$eq\": false}}&raw=true");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr =
        "[{\"quiz\":{\"nests\":{\"q2\":{\"options\":{\"[3]\":{\"this\":{\"that\":{\"them\":false}}}}}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // Multi-path
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.nests.q1,q2.options.[0]\": {\"$eq\": \"nest\"}}&raw=true");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr =
        "["
            + "{\"quiz\":{\"nests\":{\"q1\":{\"options\":{\"[0]\":\"nest\"}}}}},"
            + "{\"quiz\":{\"nests\":{\"q2\":{\"options\":{\"[0]\":\"nest\"}}}}}"
            + "]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // Multi-path...and glob?!?
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"quiz.nests.q2,q3.options.*.this.them\": {\"$eq\": false}}&raw=true");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr =
        "[{\"quiz\":{\"nests\":{\"q3\":{\"options\":{\"[2]\":{\"this\":{\"them\":false}}}}}}}]";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));
  }

  @Test
  public void testPaginationSingleDocSearch() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    Response r =
        put("/v2/namespaces/" + keyspace + "/collections/collection/cool-search-id", fullObj);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // With default page size
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"*.value\": {\"$gt\": 0}}");
    String responseBody = r.body().string();
    assertThat(responseBody).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    JsonNode responseBody1 = objectMapper.readTree(responseBody);

    assertThat(responseBody1.requiredAt("/data").size()).isEqualTo(100);
    String pageState = responseBody1.requiredAt("/pageState").requireNonNull().asText();

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"*.value\": {\"$gt\": 0}}&page-state="
                + URLEncoder.encode(pageState, "UTF-8"));
    responseBody = r.body().string();
    assertThat(responseBody).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    JsonNode responseBody2 = objectMapper.readTree(responseBody);

    assertThat(responseBody2.requiredAt("/data").size()).isEqualTo(5);
    assertThat(responseBody2.at("/pageState").isNull()).isEqualTo(true);

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
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"*.value\": {\"$gt\": 1}}&page-size=50");
    responseBody = r.body().string();
    assertThat(responseBody).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    responseBody1 = objectMapper.readTree(responseBody);

    assertThat(responseBody1.requiredAt("/data").size()).isEqualTo(50);
    pageState = responseBody1.requiredAt("/pageState").requireNonNull().asText();

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/cool-search-id?where={\"*.value\": {\"$gt\": 1}}&page-size=50&page-state="
                + URLEncoder.encode(pageState, "UTF-8"));
    responseBody = r.body().string();
    assertThat(responseBody).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    responseBody2 = objectMapper.readTree(responseBody);

    assertThat(responseBody2.requiredAt("/data").size()).isEqualTo(34);
    assertThat(responseBody2.at("/pageState").isNull()).isEqualTo(true);

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

    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", doc1);
    assertThat(r.code()).isEqualTo(200);
    r.close();
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/2", doc2);
    assertThat(r.code()).isEqualTo(200);
    r.close();
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/3", doc3);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // page-size defaults to 1 document when excluded
    r = get("/v2/namespaces/" + keyspace + "/collections/collection");
    assertThat(r.code()).isEqualTo(200);
    JsonNode resp = objectMapper.readTree(r.body().string());
    String pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    JsonNode data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    String key = data.fieldNames().next();
    assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    docsSeen.add(key);

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-state="
                + URLEncoder.encode(pageState, "UTF-8"));
    String body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(body);
    pageState = resp.requiredAt("/pageState").requireNonNull().asText();
    data = resp.requiredAt("/data");
    assertThat(data.size()).isEqualTo(1);
    // Any document could come back, find out which one is there
    key = data.fieldNames().next();
    assertThat(data.requiredAt("/" + key)).isEqualTo(docsByKey.requiredAt("/" + key));
    docsSeen.add(key);

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-state="
                + URLEncoder.encode(pageState, "UTF-8"));
    body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(body);
    assertThat(resp.at("/pageState").isNull()).isEqualTo(true);
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
    r = get("/v2/namespaces/" + keyspace + "/collections/collection?page-size=2");
    body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(body);
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
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection?page-size=2&page-state="
                + URLEncoder.encode(pageState, "UTF-8"));
    body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(body);
    assertThat(resp.at("/pageState").isNull()).isEqualTo(true);
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
    r = get("/v2/namespaces/" + keyspace + "/collections/collection?page-size=4");
    body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(body);
    assertThat(resp.at("/pageState").isNull()).isEqualTo(true);
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

    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", doc1);
    assertThat(r.code()).isEqualTo(200);
    r.close();
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/2", doc2);
    assertThat(r.code()).isEqualTo(200);
    r.close();
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/3", doc3);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // page-size defaults to 1 document when excluded
    r = get("/v2/namespaces/" + keyspace + "/collections/collection?fields=[\"a\"]");
    assertThat(r.code()).isEqualTo(200);
    JsonNode resp = objectMapper.readTree(r.body().string());
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
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection?fields=[\"a\"]&page-state="
                + URLEncoder.encode(pageState, "UTF-8"));
    String body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(body);
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
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection?fields=[\"a\"]&page-state="
                + URLEncoder.encode(pageState, "UTF-8"));
    body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(body);
    assertThat(resp.at("/pageState").isNull()).isEqualTo(true);
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
    r = get("/v2/namespaces/" + keyspace + "/collections/collection?fields=[\"a\"]&page-size=2");
    body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(body);
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
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection?fields=[\"a\"]&page-size=2&page-state="
                + URLEncoder.encode(pageState, "UTF-8"));
    body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(body);
    assertThat(resp.at("/pageState").isNull()).isEqualTo(true);
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
    r = get("/v2/namespaces/" + keyspace + "/collections/collection?fields=[\"a\"]&page-size=4");
    body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(body);
    assertThat(resp.at("/pageState").isNull()).isEqualTo(true);
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

    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", doc1);
    assertThat(r.code()).isEqualTo(200);
    r.close();
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/2", doc2);
    assertThat(r.code()).isEqualTo(200);
    r.close();
    r = put("/v2/namespaces/" + keyspace + "/collections/collection/3", doc3);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    // page-size defaults to 1 document when excluded
    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"b.value\": {\"$eq\": 2}}&fields=[\"a\"]");
    assertThat(r.code()).isEqualTo(200);
    JsonNode resp = objectMapper.readTree(r.body().string());
    assertThat(resp.at("/pageState").isNull()).isEqualTo(true);
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
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"quiz.sport.q1.question\": {\"$exists\": true}}&fields=[\"quiz\"]");
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(r.body().string());
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
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection?where={\"quiz.sport.q1.question\": {\"$exists\": true}}&fields=[\"quiz\"]&page-state="
                + URLEncoder.encode(pageState, "UTF-8"));
    assertThat(r.code()).isEqualTo(200);
    resp = objectMapper.readTree(r.body().string());
    assertThat(resp.at("/pageState").isNull()).isEqualTo(true);
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
    Response r = get("/v2/namespaces/" + keyspace + "/collections/collection?page-size=21");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).isEqualTo("The parameter `page-size` is limited to 20.");
  }

  @Test
  public void testPaginationDisallowedLimitedSupport() throws IOException {
    JsonNode doc1 =
        objectMapper.readTree(this.getClass().getClassLoader().getResource("longSearch.json"));
    Response r = put("/v2/namespaces/" + keyspace + "/collections/collection/1", doc1);
    assertThat(r.code()).isEqualTo(200);
    r.close();

    r =
        get(
            "/v2/namespaces/"
                + keyspace
                + "/collections/collection/1?where={\"*.value\":{\"$nin\": [3]}}page-size=5");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The results as requested must fit in one page, try increasing the `page-size` parameter.");
  }

  private Response get(String path) throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .get()
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    return client.newCall(request).execute();
  }

  private Response post(String path, Object arg) throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .post(
                RequestBody.create(
                    MediaType.parse("application/json"), objectMapper.writeValueAsString(arg)))
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    return client.newCall(request).execute();
  }

  private Response put(String path, Object arg) throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .put(
                RequestBody.create(
                    MediaType.parse("application/json"), objectMapper.writeValueAsString(arg)))
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    return client.newCall(request).execute();
  }

  private Response patch(String path, Object arg) throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .patch(
                RequestBody.create(
                    MediaType.parse("application/json"), objectMapper.writeValueAsString(arg)))
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    return client.newCall(request).execute();
  }

  private Response delete(String path) throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .delete()
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    return client.newCall(request).execute();
  }

  private JsonNode wrapResponse(JsonNode node, String id) {
    ObjectNode wrapperNode = objectMapper.createObjectNode();

    if (id != null) {
      wrapperNode.set("documentId", TextNode.valueOf(id));
    }
    if (node != null) {
      wrapperNode.set("data", node);
    }
    return wrapperNode;
  }

  private JsonNode wrapResponse(JsonNode node, String id, String pagingState) {
    ObjectNode wrapperNode = objectMapper.createObjectNode();

    if (id != null) {
      wrapperNode.set("documentId", TextNode.valueOf(id));
    }
    if (node != null) {
      wrapperNode.set("data", node);
    }
    wrapperNode.set("pageState", TextNode.valueOf(pagingState));
    return wrapperNode;
  }
}
