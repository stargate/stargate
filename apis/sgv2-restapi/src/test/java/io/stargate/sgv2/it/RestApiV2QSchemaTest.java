package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;

import com.google.protobuf.StringValue;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.QueryParameters;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import javax.enterprise.context.control.ActivateRequestContext;
import javax.inject.Inject;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestClassOrder(ClassOrderer.ClassName.class) // prefer stable even if arbitrary ordering
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QSchemaTest {
  @Inject protected StargateRequestInfo stargateRequestInfo;

  protected StargateBridge bridge;

  @TestHTTPResource protected String baseUrl;

  private String testKeyspaceName;

  @BeforeAll
  public void init() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @BeforeEach
  public void initPerTest(TestInfo testInfo) {
    bridge = stargateRequestInfo.getStargateBridge();
    String testName = testInfo.getTestMethod().map(ti -> ti.getName()).get();
    testKeyspaceName = "ks_" + testName + "_" + System.currentTimeMillis();

    createKeyspace(testKeyspaceName);
  }

  @Test
  public void keyspacesGetAll() throws IOException {
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
        .when()
        .get("/v2/schemas/keyspaces")
        .then()
        .statusCode(HttpStatus.SC_OK);
  }

  protected void createKeyspace(String keyspaceName) {
    String cql =
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            .formatted(keyspaceName);
    Query.Builder query =
        Query.newBuilder().setCql(cql).setParameters(QueryParameters.getDefaultInstance());
    bridge.executeQuery(query.build()).await().atMost(Duration.ofSeconds(10));
  }

  protected QueryOuterClass.Response executeCql(String cql, QueryOuterClass.Value... values) {
    return executeCql(cql, testKeyspaceName, values);
  }

  protected QueryOuterClass.Response executeCql(
      String cql, String ks, QueryOuterClass.Value... values) {
    QueryParameters parameters =
        QueryParameters.newBuilder().setKeyspace(StringValue.of(ks)).build();
    Query.Builder query = Query.newBuilder().setCql(cql).setParameters(parameters);
    if (values.length > 0) {
      query.setValues(QueryOuterClass.Values.newBuilder().addAllValues(Arrays.asList(values)));
    }
    return bridge.executeQuery(query.build()).await().atMost(Duration.ofSeconds(10));
  }
}
