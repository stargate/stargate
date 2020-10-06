package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.osgi.framework.InvalidSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
@Disabled
public class DocumentApiTest extends BaseOsgiIntegrationTest {
  private static Logger logger = LoggerFactory.getLogger(DocumentApiTest.class);

  private String keyspace;

  private static String authToken;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final OkHttpClient client = new OkHttpClient().newBuilder().build();
  private static final String host = "http://" + getStargateHost();

  public DocumentApiTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @BeforeAll
  public static void init() throws IOException {
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    RequestBody requestBody =
        RequestBody.create(
            MediaType.parse("application/json"),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")));

    Request request =
        new Request.Builder()
            .url(String.format("http://%s:8081/v1/auth/token/generate", getStargateHost()))
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

  @BeforeEach
  public void setup(TestInfo info)
      throws InvalidSyntaxException, ExecutionException, InterruptedException {
    String testName = info.getTestMethod().get().getName();
    if (testName.indexOf('[') >= 0) testName = testName.substring(0, testName.indexOf('['));
    keyspace = "ks_" + testName;

    Persistence persistence = getOsgiService("io.stargate.db.Persistence", Persistence.class);
    ClientState clientState = persistence.newClientState("");
    QueryState queryState = persistence.newQueryState(clientState);
    DataStore dataStore = persistence.newDataStore(queryState, null);

    ResultSet result =
        dataStore
            .query()
            .create()
            .keyspace(keyspace)
            .ifNotExists()
            .withReplication("{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
            .andDurableWrites(true)
            .execute();

    dataStore.waitForSchemaAgreement();
  }

  @Test
  public void testIt() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    Response r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    r = get("/v1/" + keyspace + "/collection/1/quiz/maths");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj.requiredAt("/quiz/maths"));

    r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection/products?where={\"price\":{\"$lt\": 800}}&recurse=true");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            objectMapper.readTree(
                this.getClass().getResource("test/resources/exampleSearchResult.json")));
  }

  @Test
  public void testInvalidKeyPut() throws IOException {
    JsonNode obj = objectMapper.readTree("{ \"square[]braces\": \"are not allowed\" }");

    Response r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field square[]braces");

    obj = objectMapper.readTree("{ \"commas,\": \"are not allowed\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field commas,");

    obj = objectMapper.readTree("{ \"periods.\": \"are not allowed\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field periods.");

    obj = objectMapper.readTree("{ \"'quotes'\": \"are not allowed\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field 'quotes'");

    obj = objectMapper.readTree("{ \"*asterisks*\": \"are not allowed\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "The characters [`[`, `]`, `,`, `.`, `'`, `*`] are not permitted in JSON field names, invalid field *asterisks*");
  }

  @Test
  public void testWeirdButAllowedKeys() throws IOException {
    JsonNode obj = objectMapper.readTree("{ \"$\": \"weird but allowed\" }");
    Response r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{ \"$30\": \"not as weird\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{ \"@\": \"weird but allowed\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{ \"meet me @ the place\": \"not as weird\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{ \"?\": \"weird but allowed\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{ \"spac es\": \"weird but allowed\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{ \"3\": [\"totally allowed\"] }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    r = get("/v1/" + keyspace + "/collection/1/3/[0]");
    assertThat(r.body().string()).isEqualTo("\"totally allowed\"");

    obj = objectMapper.readTree("{ \"-1\": \"totally allowed\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    r = get("/v1/" + keyspace + "/collection/1/-1");
    assertThat(r.body().string()).isEqualTo("\"totally allowed\"");

    obj = objectMapper.readTree("{ \"Eric says \\\"hello\\\"\": \"totally allowed\" }");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);
  }

  @Test
  public void testInvalidDepthAndLength() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getResource("test/resources/tooDeep.json"));
    Response r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).isEqualTo("Max depth of 64 exceeded");

    obj = objectMapper.readTree("{ \"some\": \"json\" }");
    r = put("/v1/" + keyspace + "/collection/1/[1000000]", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).isEqualTo("Max array length of 1000000 exceeded.");
  }

  @Test
  public void testArrayGet() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    Response r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.body().string()).isEqualTo("");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1/quiz/maths/q1/options/[0]");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(obj.requiredAt("/quiz/maths/q1/options/0"));

    r = get("/v1/" + keyspace + "/collection/1/quiz/nests/q1/options/[3]/this");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(obj.requiredAt("/quiz/nests/q1/options/3/this"));
  }

  @Test
  public void testInvalidPathGet() throws IOException {
    JsonNode obj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    Response r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.body().string()).isEqualTo("");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1/nonexistent/path");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v1/" + keyspace + "/collection/1/nonexistent/path/[1]");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v1/" + keyspace + "/collection/1/quiz/maths/q1/options/[9999]"); // out of bounds
    assertThat(r.code()).isEqualTo(204);
  }

  @Test
  public void testRootDocumentPut() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": 1}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    Response r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": true}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": {\"a\": {\"b\": 0 }}}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": [1,2,3,4]}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": [1,{\"a\": 23},3,4]}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": [0]}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"done\": \"done\"}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);
  }

  @Test
  public void testRootDocumentPutNulls() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": null}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    Response r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": null}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": {\"a\": {\"b\": null }}}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": [null,2,null,4]}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": [1,{\"a\": null},3,4]}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": [null]}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"null\": null}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);
  }

  @Test
  public void testPutReplacingObject() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    Response r = put("/v1/" + keyspace + "/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(fullObj);

    JsonNode obj;
    obj = objectMapper.readTree("{\"q5000\": \"hello?\"}");
    assertThat(put("/v1/" + keyspace + "/collection/1/quiz/sport", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1/quiz/sport");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);

    ObjectNode sportNode = objectMapper.createObjectNode();
    sportNode.set("q5000", TextNode.valueOf("hello?"));

    ObjectNode fullObjNode = (ObjectNode) fullObj;
    ((ObjectNode) fullObjNode.get("quiz")).set("sport", sportNode);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(fullObjNode);
  }

  @Test
  public void testPutReplacingArrayElement() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    Response r = put("/v1/" + keyspace + "/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(fullObj);

    JsonNode obj;
    obj = objectMapper.readTree("{\"q5000\": \"hello?\"}");
    assertThat(put("/v1/" + keyspace + "/collection/1/quiz/nests/q1/options/[0]", obj).code())
        .isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1/quiz/nests/q1/options/[0]");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);

    ObjectNode optionNode = objectMapper.createObjectNode();
    optionNode.set("q5000", TextNode.valueOf("hello?"));

    ObjectNode fullObjNode = (ObjectNode) fullObj;
    ((ArrayNode) fullObjNode.at("/quiz/nests/q1/options")).set(0, optionNode);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(fullObjNode);
  }

  @Test
  public void testPutReplacingWithArray() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    Response r = put("/v1/" + keyspace + "/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(fullObj);

    JsonNode obj = objectMapper.readTree("[{\"array\": \"at\"}, \"sub\", \"doc\"]");
    r = put("/v1/" + keyspace + "/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1/quiz");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("[0, \"a\", \"2\", true]");
    assertThat(put("/v1/" + keyspace + "/collection/1/quiz/nests/q1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1/quiz/nests/q1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    r = get("/v1/" + keyspace + "/collection/1");
    ObjectNode nestsNode =
        (ObjectNode) objectMapper.readTree("{\"nests\":{\"q1\":[0,\"a\",\"2\",true]}}");
    ObjectNode fullObjNode = (ObjectNode) fullObj;
    fullObjNode.set("quiz", nestsNode);

    String body = r.body().string();
    assertThat(body).startsWith("{");
    assertThat(objectMapper.readTree(body)).isEqualTo(fullObjNode);
    assertThat(r.code()).isEqualTo(200);

    obj = objectMapper.readTree("[{\"array\": \"at\"}, \"root\", \"doc\"]");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"we\": {\"are\": \"done\"}}");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);
  }

  @Test
  public void testInvalidPuts() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    Response r = put("/v1/" + keyspace + "/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(fullObj);

    JsonNode obj;
    obj = objectMapper.readTree("3");
    r = put("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: 3\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("true");
    r = put("/v1/" + keyspace + "/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: true\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("null");
    r = put("/v1/" + keyspace + "/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: null\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("\"Eric\"");
    r = put("/v1/" + keyspace + "/collection/1/quiz/sport", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: \"Eric\"\nHint: update the parent path with a defined object instead.");
  }

  @Test
  public void testDelete() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    Response r = put("/v1/" + keyspace + "/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(fullObj);

    r = delete("/v1/" + keyspace + "/collection/1/quiz/sport/q1/question");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v1/" + keyspace + "/collection/1/quiz/sport/q1/question");
    assertThat(r.code()).isEqualTo(204);

    r = delete("/v1/" + keyspace + "/collection/1/quiz/maths");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v1/" + keyspace + "/collection/1/quiz/maths");
    assertThat(r.code()).isEqualTo(204);

    r = delete("/v1/" + keyspace + "/collection/1/quiz/nests/q1/options/[0]");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v1/" + keyspace + "/collection/1/quiz/nests/q1/options");
    assertThat(r.code()).isEqualTo(200);
    String responseBody = r.body().string();
    assertThat(objectMapper.readTree(responseBody))
        .isEqualTo(
            objectMapper.readTree(
                "[null,\"not a nest\",\"definitely not a nest\",{ \"this\":  true }]"));

    r = delete("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(204);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(204);
  }

  @Test
  public void testPost() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    Response resp = post("/v1/" + keyspace + "/collection", fullObj);
    assertThat(resp.code()).isEqualTo(201);

    String newLocation = resp.header("location");
    String body = resp.body().string();
    assertThat(objectMapper.readTree(body).requiredAt("/id")).isNotNull();

    resp = get(newLocation.replace(host + ":8090", ""));
    assertThat(resp.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(resp.body().string())).isEqualTo(fullObj);
  }

  @Test
  public void testRootDocumentPatch() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": 1}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    Response r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": true}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": 1, \"bcd\": true }"));

    obj = objectMapper.readTree("{\"bcd\": {\"a\": {\"b\": 0 }}}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": 1, \"bcd\": {\"a\": {\"b\": 0 }} }"));

    obj = objectMapper.readTree("{\"bcd\": [1,2,3,4]}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": 1, \"bcd\": [1,2,3,4] }"));

    obj = objectMapper.readTree("{\"bcd\": [5,{\"a\": 23},7,8]}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": 1, \"bcd\": [5,{\"a\": 23},7,8] }"));

    obj = objectMapper.readTree("{\"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            objectMapper.readTree(
                "{ \"abc\": 1, \"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] }"));

    obj = objectMapper.readTree("{\"bcd\": {\"replace\": \"array\"}}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"} }"));

    obj = objectMapper.readTree("{\"done\": \"done\"}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            objectMapper.readTree(
                "{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"}, \"done\": \"done\" }"));
  }

  @Test
  public void testRootDocumentPatchNulls() throws IOException {
    JsonNode obj = objectMapper.readTree("{\"abc\": null}");
    assertThat(put("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    Response r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(obj);

    obj = objectMapper.readTree("{\"bcd\": null}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": null, \"bcd\": null }"));

    obj = objectMapper.readTree("{\"bcd\": {\"a\": {\"b\": null }}}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": null, \"bcd\": {\"a\": {\"b\": null }} }"));

    obj = objectMapper.readTree("{\"bcd\": [null,2,null,4]}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": null, \"bcd\": [null,2,null,4] }"));

    obj = objectMapper.readTree("{\"bcd\": [1,{\"a\": null},3,4]}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": null, \"bcd\": [1,{\"a\": null},3,4] }"));

    obj = objectMapper.readTree("{\"bcd\": [null]}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": null, \"bcd\": [null] }"));

    obj = objectMapper.readTree("{\"null\": null}");
    assertThat(patch("/v1/" + keyspace + "/collection/1", obj).code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree("{ \"abc\": null, \"bcd\": [null], \"null\": null }"));
  }

  @Test
  public void testInvalidPatches() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    Response r = put("/v1/" + keyspace + "/collection/1", fullObj);
    assertThat(r.body().string()).isEqualTo("");
    assertThat(r.code()).isEqualTo(200);

    r = get("/v1/" + keyspace + "/collection/1");
    assertThat(r.code()).isEqualTo(200);
    assertThat(objectMapper.readTree(r.body().string())).isEqualTo(fullObj);

    JsonNode obj;
    obj = objectMapper.readTree("[{\"array\": \"at\"}, \"root\", \"doc\"]");
    r = patch("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo("A patch operation must be done with a JSON object, not an array.");

    // For patching, you must use an object, so arrays even patched to sub-paths are not allowed.
    r = patch("/v1/" + keyspace + "/collection/1/quiz/sport/q1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo("A patch operation must be done with a JSON object, not an array.");

    obj = objectMapper.readTree("3");
    r = patch("/v1/" + keyspace + "/collection/1", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: 3\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("true");
    r = patch("/v1/" + keyspace + "/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: true\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("null");
    r = patch("/v1/" + keyspace + "/collection/1/quiz", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: null\nHint: update the parent path with a defined object instead.");

    obj = objectMapper.readTree("\"Eric\"");
    r = patch("/v1/" + keyspace + "/collection/1/quiz/sport", obj);
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: \"Eric\"\nHint: update the parent path with a defined object instead.");
  }

  @Test
  public void testBasicSearch() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    assertThat(put("/v1/" + keyspace + "/collection/cool-search-id", fullObj).code())
        .isEqualTo(200);
    assertThat(put("/v1/" + keyspace + "/collection/cool-search-id-2", fullObj).code())
        .isEqualTo(200);

    // EQ
    Response r = get("/v1/query/" + keyspace + "/collection?where={\"price\": {\"$eq\": 600}}");
    assertThat(r.code()).isEqualTo(200);

    String searchResultStr = "{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            objectMapper.readTree(
                String.format(
                    "{ \"cool-search-id\": %s, \"cool-search-id-2\": %s}",
                    searchResultStr, searchResultStr)));

    r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection?where={\"price\": {\"$eq\": 600}}&recurse=false");
    assertThat(r.code()).isEqualTo(204);

    // LT
    r = get("/v1/query/" + keyspace + "/collection?where={\"price\": {\"$lt\": 600}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99}, \"Pear\": {\"price\": 0.89}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            objectMapper.readTree(
                String.format(
                    "{ \"cool-search-id\": %s, \"cool-search-id-2\": %s}",
                    searchResultStr, searchResultStr)));

    // LTE
    r = get("/v1/query/" + keyspace + "/collection?where={\"price\": {\"$lte\": 600}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}, \"food\": {\"Apple\": {\"price\": 0.99}, \"Pear\": {\"price\": 0.89}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            objectMapper.readTree(
                String.format(
                    "{ \"cool-search-id\": %s, \"cool-search-id-2\": %s}",
                    searchResultStr, searchResultStr)));

    // GT
    r = get("/v1/query/" + keyspace + "/collection?where={\"price\": {\"$gt\": 600}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "{\"products\": {\"electronics\": {\"iPhone_11\": {\"price\": 900}}, \"food\": {\"Orange\": {\"info\": {\"price\": 600.01}}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            objectMapper.readTree(
                String.format(
                    "{ \"cool-search-id\": %s, \"cool-search-id-2\": %s}",
                    searchResultStr, searchResultStr)));

    // GTE
    r = get("/v1/query/" + keyspace + "/collection?where={\"price\": {\"$gte\": 600}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}, \"iPhone_11\": {\"price\": 900}}, \"food\": {\"Orange\": {\"info\": {\"price\": 600.01}}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            objectMapper.readTree(
                String.format(
                    "{ \"cool-search-id\": %s, \"cool-search-id-2\": %s}",
                    searchResultStr, searchResultStr)));

    // EXISTS
    r = get("/v1/query/" + keyspace + "/collection?where={\"price\": {\"$exists\": true}}");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}, \"iPhone_11\": {\"price\": 900}}, \"food\": {\"Apple\": {\"price\": 0.99}, \"Pear\": {\"price\": 0.89}, \"Orange\": {\"info\": {\"price\": 600.01}}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            objectMapper.readTree(
                String.format(
                    "{ \"cool-search-id\": %s, \"cool-search-id-2\": %s}",
                    searchResultStr, searchResultStr)));

    // Filter to just one document
    r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection?where={\"price\": {\"$exists\": true}}&documentKey=cool-search-id-2");
    assertThat(r.code()).isEqualTo(200);

    searchResultStr =
        "{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}, \"iPhone_11\": {\"price\": 900}}, \"food\": {\"Apple\": {\"price\": 0.99}, \"Pear\": {\"price\": 0.89}, \"Orange\": {\"info\": {\"price\": 600.01}}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(
            objectMapper.readTree(
                String.format("{\"cool-search-id-2\": %s}", searchResultStr, searchResultStr)));
  }

  @Test
  public void testInvalidSearch() throws IOException {
    Response r = get("/v1/query/" + keyspace + "/collection?where=hello");
    assertThat(r.code()).isEqualTo(500);

    r = get("/v1/query/" + keyspace + "/collection?where=[\"a\"]}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).isEqualTo("Search was expecting a JSON object as input.");

    r = get("/v1/query/" + keyspace + "/collection?where={\"a\": true}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo("Search entry for field a was expecting a JSON object as input.");

    r = get("/v1/query/" + keyspace + "/collection?where={\"a\": {\"$exists\": false}}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string()).isEqualTo("`exists` only supports the value `true`");

    r = get("/v1/query/" + keyspace + "/collection?where={\"a\": {\"$eq\": null}}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Value entry for field a, operation eq was expecting a value, but found an object, array, or null.");

    r = get("/v1/query/" + keyspace + "/collection?where={\"a\": {\"$eq\": {}}}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Value entry for field a, operation eq was expecting a value, but found an object, array, or null.");

    r = get("/v1/query/" + keyspace + "/collection?where={\"a\": {\"$eq\": []}}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .isEqualTo(
            "Value entry for field a, operation eq was expecting a value, but found an object, array, or null.");

    r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection?where={\"a\": {\"$eq\": 300}, \"b\": {\"lt\": 500}}");
    assertThat(r.code()).isEqualTo(400);
    assertThat(r.body().string())
        .contains("Conditions across multiple fields are not yet supported");
  }

  @Test
  public void testMultiSearch() throws IOException {
    JsonNode fullObj =
        objectMapper.readTree(this.getClass().getResource("test/resources/example.json"));
    assertThat(put("/v1/" + keyspace + "/collection/cool-search-id", fullObj).code())
        .isEqualTo(200);

    // Multiple operators
    Response r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection?where={\"price\": {\"gt\": 600, \"$lt\": 600.05}}");
    assertThat(r.code()).isEqualTo(200);

    String searchResultStr =
        "{\"cool-search-id\":{\"products\": {\"food\": {\"Orange\": {\"info\": {\"price\": 600.01}}}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // Array paths
    r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection/quiz/maths/q1/options?where={\"[0]\": {\"$lt\": 13.3}}");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr = "{\"cool-search-id\":{\"[0]\":10.2}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection/quiz/nests/q1/options/[3]?where={\"this\": {\"$eq\": true}}");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr = "{\"cool-search-id\":{\"this\":true}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection/quiz/nests/q1/options/[*]?where={\"this\": {\"$eq\": true}}");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr = "{\"cool-search-id\":{\"[3]\":{\"this\":true}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection/quiz/nests/q2/options/[*]/this?where={\"them\": {\"$eq\": false}}&recurse=true");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr = "{\"cool-search-id\":{\"[3]\":{\"this\":{\"that\":{\"them\":false}}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // Multi-path
    r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection/quiz/nests/q1,q2/options?where={\"[0]\": {\"$eq\": \"nest\"}}");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr =
        "{\"cool-search-id\":{\"q1\":{\"options\":{\"[0]\":\"nest\"}},\"q2\":{\"options\":{\"[0]\":\"nest\"}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));

    // Multi-path...and glob?!?
    r =
        get(
            "/v1/query/"
                + keyspace
                + "/collection/quiz/nests/q2,q3/options/[*]/this?where={\"them\": {\"$eq\": false}}&recurse=true");
    assertThat(r.code()).isEqualTo(200);
    searchResultStr =
        "{\"cool-search-id\":{\"q2\":{\"options\":{\"[3]\":{\"this\":{\"that\":{\"them\":false}}}}},\"q3\":{\"options\":{\"[2]\":{\"this\":{\"them\":false}},\"[3]\":{\"this\":{\"that\":{\"them\":false}}}}}}}";
    assertThat(objectMapper.readTree(r.body().string()))
        .isEqualTo(objectMapper.readTree(searchResultStr));
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
}
