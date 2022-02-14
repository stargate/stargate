package io.stargate.it.http.docsapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.ClusterConnectionInfo;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.time.Duration;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@NotThreadSafe
public class CollectionsResourceIntTest extends BaseIntegrationTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final OkHttpClient CLIENT =
      new OkHttpClient().newBuilder().readTimeout(Duration.ofMinutes(3)).build();

  private static String authToken;
  private static String host;
  private static String hostWithPort;
  private static boolean isDse;

  @BeforeAll
  public static void setup(ClusterConnectionInfo backend, StargateConnectionInfo stargate)
      throws IOException {
    host = "http://" + stargate.seedAddress();
    hostWithPort = host + ":" + DEFAULT_DOCS_API_PORT;
    isDse = backend.isDse();

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

  String getBasePath(CqlIdentifier keyspace) {
    return hostWithPort + "/v2/namespaces/" + keyspace + "/collections";
  }

  @Nested
  @CqlSessionSpec
  @ExtendWith(CqlSessionExtension.class)
  class GetCollections {

    @Test
    public void raw(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String r = RestUtils.get(authToken, getBasePath(keyspace) + "?raw=true", 200);

      String expected = "[]";
      assertThat(r).isEqualTo(expected);
    }

    @Test
    public void notRaw(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String r = RestUtils.get(authToken, getBasePath(keyspace) + "?raw=false", 200);

      String expected = "{\"data\":[]}";
      assertThat(r).isEqualTo(expected);
    }

    @Test
    public void variedTables(CqlSession session, @TestKeyspace CqlIdentifier keyspace)
        throws IOException {
      assertThat(
              session
                  .execute(
                      String.format(
                          "create table \"%s\".not_docs(x text primary key, y text)", keyspace))
                  .wasApplied())
          .isTrue();
      String basePath = getBasePath(keyspace);
      String newColl = "{\"name\": \"newcollection\"}";
      RestUtils.post(authToken, basePath, newColl, 201);

      String result = RestUtils.get(authToken, getBasePath(keyspace) + "?raw=true", 200);

      // non-docs tables should be excluded
      assertThat(result).isEqualTo("[{\"name\":\"newcollection\",\"upgradeAvailable\":false}]");

      assertThat(
              session.execute(String.format("drop table \"%s\".not_docs", keyspace)).wasApplied())
          .isTrue();
      assertThat(
              session
                  .execute(String.format("drop table \"%s\".newcollection", keyspace))
                  .wasApplied())
          .isTrue();
    }
  }

  @Nested
  @CqlSessionSpec
  @ExtendWith(CqlSessionExtension.class)
  class CreateCollection {

    @Test
    public void created(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      // Create a brand new collection
      String payload = "{\"name\": \"newcollection\"}";

      RestUtils.post(authToken, getBasePath(keyspace), payload, 201);

      String r = RestUtils.get(authToken, getBasePath(keyspace) + "?raw=true", 200);
      String expected = "[{\"name\":\"newcollection\",\"upgradeAvailable\":false}]";
      assertThat(r).isEqualTo(expected);
    }

    @Test
    public void noPayload(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String r = RestUtils.post(authToken, getBasePath(keyspace), null, 422);

      assertThat(r)
          .isEqualTo("{\"description\":\"Request invalid: payload not provided.\",\"code\":422}");
    }

    @Test
    public void malformedPayload(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String r = RestUtils.post(authToken, getBasePath(keyspace), "{\"malformed\":", 400);

      assertThat(r).isEqualTo("{\"code\":400,\"message\":\"Unable to process JSON\"}");
    }

    @Test
    public void invalidPayload(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      // no name in the json
      String payload = "{}";

      String r = RestUtils.post(authToken, getBasePath(keyspace), payload, 422);

      assertThat(r)
          .isEqualTo(
              "{\"description\":\"Request invalid: `name` is required to create a collection.\",\"code\":422}");
    }
  }

  @Nested
  @CqlSessionSpec
  @ExtendWith(CqlSessionExtension.class)
  class DeleteCollection {

    @Test
    public void deleted(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String basePath = getBasePath(keyspace);
      String newColl = "{\"name\": \"newcollection\"}";
      RestUtils.post(authToken, basePath, newColl, 201);

      RestUtils.delete(authToken, basePath + "/newcollection", 204);
    }

    @Test
    public void notExisting(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String result = RestUtils.delete(authToken, getBasePath(keyspace) + "/notexisting", 404);

      assertThat(result)
          .isEqualTo("{\"description\":\"Collection 'notexisting' not found.\",\"code\":404}");
    }

    @Test
    public void notValidCollection(CqlSession session, @TestKeyspace CqlIdentifier keyspace)
        throws IOException {
      assertThat(
              session
                  .execute(
                      String.format(
                          "create table \"%s\".not_docs(x text primary key, y text)", keyspace))
                  .wasApplied())
          .isTrue();
      String result = RestUtils.delete(authToken, getBasePath(keyspace) + "/not_docs", 404);

      assertThat(result)
          .isEqualTo("{\"description\":\"Collection 'not_docs' not found.\",\"code\":404}");
    }
  }

  @Nested
  @CqlSessionSpec
  @ExtendWith(CqlSessionExtension.class)
  class UpgradeCollection {

    @Test
    public void upgrade(@TestKeyspace CqlIdentifier keyspace, CqlSession cqlSession)
        throws IOException {
      // ignore if not dse
      assumeTrue(isDse, "Test disabled when not running on DSE");

      // Create a brand new collection, it should already have SAI so drop it before calling
      String newColl = "{\"name\": \"newcollection\"}";
      RestUtils.post(authToken, getBasePath(keyspace), newColl, 201);
      dropIndexes(cqlSession, keyspace, "newcollection"); //

      String upgradeAction = "{\"upgradeType\": \"SAI_INDEX_UPGRADE\"}";
      String r =
          RestUtils.post(
              authToken,
              getBasePath(keyspace) + "/newcollection/upgrade?raw=true",
              upgradeAction,
              200);

      assertThat(r).isEqualTo("{\"name\":\"newcollection\",\"upgradeAvailable\":false}");
    }

    @Test
    public void upgradeNotAvailable(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      // ignore if not dse
      assumeTrue(isDse, "Test disabled when not running on DSE");

      // Create a brand new collection, it should already have SAI so it requires no upgrade
      String newColl = "{\"name\": \"othercollection\"}";
      RestUtils.post(authToken, getBasePath(keyspace), newColl, 201);

      String upgradeAction = "{\"upgradeType\": \"SAI_INDEX_UPGRADE\"}";
      String r =
          RestUtils.post(
              authToken, getBasePath(keyspace) + "/othercollection/upgrade", upgradeAction, 400);

      assertThat(r)
          .isEqualTo(
              "{\"description\":\"The collection cannot be upgraded in given manner.\",\"code\":400}");
    }

    @Test
    public void noPayload(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      String r =
          RestUtils.post(authToken, getBasePath(keyspace) + "/newcollection/upgrade", null, 422);

      assertThat(r)
          .isEqualTo("{\"description\":\"Request invalid: payload not provided.\",\"code\":422}");
    }

    @Test
    public void invalidPayload(@TestKeyspace CqlIdentifier keyspace) throws IOException {
      // no name in the json
      String payload = "{\"upgradeType\": null}";

      String r =
          RestUtils.post(authToken, getBasePath(keyspace) + "/newcollection/upgrade", payload, 422);

      assertThat(r)
          .isEqualTo(
              "{\"description\":\"Request invalid: `upgradeType` is required to upgrade a collection.\",\"code\":422}");
    }

    private void dropIndexes(CqlSession session, CqlIdentifier keyspace, String collection) {
      session.execute(
          String.format("DROP INDEX \"%s\".\"%s\"", keyspace, collection + "_leaf_idx"));

      session.execute(
          String.format("DROP INDEX \"%s\".\"%s\"", keyspace, collection + "_text_value_idx"));

      session.execute(
          String.format("DROP INDEX \"%s\".\"%s\"", keyspace, collection + "_dbl_value_idx"));

      session.execute(
          String.format("DROP INDEX \"%s\".\"%s\"", keyspace, collection + "_bool_value_idx"));
    }
  }
}
