package io.stargate.it.http.restapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.web.restapi.models.GetResponseWrapper;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/** Intermediate IT base class for use by REST API integration tests. */
public class RestApiTestBase extends BaseIntegrationTest {
  protected String keyspaceName;
  protected String tableName;
  protected static String authToken;
  private String host;
  protected String restUrlBase;

  protected static class ListOfMapsGetResponseWrapper
      extends GetResponseWrapper<List<Map<String, Object>>> {
    public ListOfMapsGetResponseWrapper() {
      super(-1, null, null);
    }
  }

  protected static final ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  void setup(TestInfo testInfo, StargateConnectionInfo cluster) throws IOException {
    host = "http://" + cluster.seedAddress();
    restUrlBase = host + ":8082";

    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
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
  }

  /*
  /************************************************************************
  /* Shared helper methods for setting up tests
  /************************************************************************
   */

  protected void createTestKeyspace(String keyspaceName) {
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
}
