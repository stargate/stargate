package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.web.models.Keyspace;
import io.stargate.web.restapi.models.RESTResponseWrapper;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Optional;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for REST API v2 that cover CRUD operations against schema, but not actual Row
 * data.
 */
@NotThreadSafe
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec()
@ExtendWith(ApiServiceExtension.class)
@ApiServiceSpec()
public class RestApiv2SchemaTest extends BaseIntegrationTest {
  private String keyspaceName;
  private String tableName;
  private static String authToken;
  private String restUrlBase;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  public void setup(
      TestInfo testInfo, StargateConnectionInfo cluster, ApiServiceConnectionInfo restApi)
      throws IOException {
    restUrlBase = "http://" + restApi.host() + ":" + restApi.port();
    String authUrlBase =
        "http://" + cluster.seedAddress() + ":8081"; // TODO: make auth port configurable

    String body =
        RestUtils.post(
            "",
            String.format("%s/v1/auth/token/generate", authUrlBase),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();

    Optional<String> name = testInfo.getTestMethod().map(Method::getName);
    assertThat(name).isPresent();
    String testName = name.get();

    keyspaceName = "ks_" + testName + "_" + System.currentTimeMillis();
    tableName = "tbl_" + testName + "_" + System.currentTimeMillis();

    // TODO: temporarily enforcing lower case names,
    // should remove to ensure support for mixed case identifiers
    //    keyspaceName = "ks_" + testName.toLowerCase() + "_" + System.currentTimeMillis();
    //    tableName = "tbl_" + testName.toLowerCase() + "_" + System.currentTimeMillis();
  }

  /*
  /************************************************************************
  /* Test methods for Keyspace CRUD operations
  /************************************************************************
   */

  @Test
  public void keyspacesGetAll() throws IOException {
    String body =
        RestUtils.get(
            authToken, String.format("%s/v2/schemas/keyspaces", restUrlBase), HttpStatus.SC_OK);

    Keyspace[] keyspaces = readWrappedRESTResponse(body, Keyspace[].class);
    assertThat(keyspaces)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .isEqualTo(new Keyspace("system", null)));
  }

  @Test
  public void keyspacesGetAllRaw() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces?raw=true", restUrlBase),
            HttpStatus.SC_OK);

    Keyspace[] keyspaces = objectMapper.readValue(body, Keyspace[].class);
    assertThat(keyspaces)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .isEqualTo(new Keyspace("system_schema", null)));
  }

  @Test
  public void keyspacesGetAllMissingToken() throws IOException {
    RestUtils.get(
        "", String.format("%s/v2/schemas/keyspaces", restUrlBase), HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void keyspacesGetAllBadToken() throws IOException {
    RestUtils.get(
        "foo", String.format("%s/v2/schemas/keyspaces", restUrlBase), HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void keyspaceGetWrapped() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces/system", restUrlBase),
            HttpStatus.SC_OK);
    Keyspace keyspace = readWrappedRESTResponse(body, Keyspace.class);
    assertThat(keyspace).usingRecursiveComparison().isEqualTo(new Keyspace("system", null));
  }

  @Test
  public void keyspaceGetRaw() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces/system?raw=true", restUrlBase),
            HttpStatus.SC_OK);

    Keyspace keyspace = objectMapper.readValue(body, Keyspace.class);
    assertThat(keyspace).usingRecursiveComparison().isEqualTo(new Keyspace("system", null));
  }

  @Test
  public void keyspaceGetNotFound() throws IOException {
    RestUtils.get(
        authToken,
        String.format("%s/v2/schemas/keyspaces/ks_not_found", restUrlBase),
        HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void keyspaceCreate() throws IOException {
    String keyspaceName = "ks_createkeyspace_" + System.currentTimeMillis();
    createTestKeyspace(keyspaceName);

    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces/%s?raw=true", restUrlBase, keyspaceName),
            HttpStatus.SC_OK);

    Keyspace keyspace = objectMapper.readValue(body, Keyspace.class);

    assertThat(keyspace).usingRecursiveComparison().isEqualTo(new Keyspace(keyspaceName, null));
  }

  @Test
  public void keyspaceCreateWithInvalidJson() throws IOException {
    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces", restUrlBase),
        "{\"name\" \"badjsonkeyspace\", \"replicas\": 1}",
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void keyspaceDelete() throws IOException {
    String keyspaceName = "ks_createkeyspace_" + System.currentTimeMillis();
    createTestKeyspace(keyspaceName);

    RestUtils.get(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s", restUrlBase, keyspaceName),
        HttpStatus.SC_OK);

    RestUtils.delete(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s", restUrlBase, keyspaceName),
        HttpStatus.SC_NO_CONTENT);

    RestUtils.get(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s", restUrlBase, keyspaceName),
        HttpStatus.SC_NOT_FOUND);
  }

  /*
  /************************************************************************
  /* Helper methods for setting up tests
  /************************************************************************
   */

  private void createTestKeyspace(String keyspaceName) {
    String createKeyspaceRequest =
        String.format("{\"name\": \"%s\", \"replicas\": 1}", keyspaceName);

    try {
      RestUtils.post(
          authToken,
          String.format("%s/v2/schemas/keyspaces", restUrlBase),
          createKeyspaceRequest,
          HttpStatus.SC_CREATED);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T readWrappedRESTResponse(String body, Class<T> wrappedType) {
    JavaType wrapperType =
        objectMapper
            .getTypeFactory()
            .constructParametricType(RESTResponseWrapper.class, wrappedType);
    try {
      RESTResponseWrapper<T> wrapped = objectMapper.readValue(body, wrapperType);
      return wrapped.getData();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
