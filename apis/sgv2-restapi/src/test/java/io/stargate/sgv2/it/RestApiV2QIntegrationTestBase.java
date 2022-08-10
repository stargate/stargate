package io.stargate.sgv2.it;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.protobuf.StringValue;
import io.quarkus.test.common.http.TestHTTPResource;
import io.restassured.RestAssured;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.restapi.service.models.Sgv2RESTResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public class RestApiV2QIntegrationTestBase {
  protected static final ObjectMapper objectMapper = JsonMapper.builder().build();

  @Inject protected StargateRequestInfo stargateRequestInfo;

  protected StargateBridge bridge;

  @TestHTTPResource protected String baseUrl;

  private final String testKeyspacePrefix;

  private final String testTablePrefix;

  private String testKeyspaceName;

  private String testTableName;

  /*
  /////////////////////////////////////////////////////////////////////////
  // Initialization
  /////////////////////////////////////////////////////////////////////////
   */

  protected RestApiV2QIntegrationTestBase(String keyspacePrefix, String tablePrefix) {
    this.testKeyspacePrefix = keyspacePrefix;
    this.testTablePrefix = tablePrefix;
  }

  @BeforeAll
  public void init() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @BeforeEach
  public void initPerTest(TestInfo testInfo) {
    bridge = stargateRequestInfo.getStargateBridge();
    // Let's force lower-case keyspace and table names for defaults; case-sensitive testing
    // needs to use explicitly different values
    String testName = testInfo.getTestMethod().map(ti -> ti.getName()).get(); // .toLowerCase();
    testKeyspaceName = testKeyspacePrefix + testName + "_" + System.currentTimeMillis();
    testTableName = testTablePrefix + testName + System.currentTimeMillis();

    // Create keyspace automatically (same as before)
    createKeyspace(testKeyspaceName);
    // But not table (won't have definition anyway)
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Accessors
  /////////////////////////////////////////////////////////////////////////
   */

  public String testKeyspaceName() {
    return testKeyspaceName;
  }

  public String testTableName() {
    return testTableName;
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Schema initialization
  /////////////////////////////////////////////////////////////////////////
   */

  protected void createKeyspace(String keyspaceName) {
    String cql =
        "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            .formatted(keyspaceName);
    QueryOuterClass.Query.Builder query =
        QueryOuterClass.Query.newBuilder()
            .setCql(cql)
            .setParameters(QueryOuterClass.QueryParameters.getDefaultInstance());
    bridge.executeQuery(query.build()).await().atMost(Duration.ofSeconds(10));
  }

  protected QueryOuterClass.Response executeCql(String cql, QueryOuterClass.Value... values) {
    return executeCql(cql, testKeyspaceName, values);
  }

  protected QueryOuterClass.Response executeCql(
      String cql, String ks, QueryOuterClass.Value... values) {
    QueryOuterClass.QueryParameters parameters =
        QueryOuterClass.QueryParameters.newBuilder().setKeyspace(StringValue.of(ks)).build();
    QueryOuterClass.Query.Builder query =
        QueryOuterClass.Query.newBuilder().setCql(cql).setParameters(parameters);
    if (values.length > 0) {
      query.setValues(QueryOuterClass.Values.newBuilder().addAllValues(Arrays.asList(values)));
    }
    return bridge.executeQuery(query.build()).await().atMost(Duration.ofSeconds(10));
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // JSON handling
  /////////////////////////////////////////////////////////////////////////
   */

  protected <T> T readWrappedRESTResponse(String body, Class<T> wrappedType) {
    JavaType wrapperType =
        objectMapper.getTypeFactory().constructParametricType(Sgv2RESTResponse.class, wrappedType);
    try {
      Sgv2RESTResponse<T> wrapped = objectMapper.readValue(body, wrapperType);
      return wrapped.getData();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected <T> T readJsonAs(String body, Class<T> asType) {
    try {
      return objectMapper.readValue(body, asType);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected String asJsonString(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
