package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.protobuf.StringValue;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.restapi.service.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restapi.service.models.Sgv2GetResponse;
import io.stargate.sgv2.restapi.service.models.Sgv2RESTResponse;
import io.stargate.sgv2.restapi.service.models.Sgv2Table;
import io.stargate.sgv2.restapi.service.models.Sgv2TableAddRequest;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public class RestApiV2QIntegrationTestBase {
  protected static final ObjectMapper objectMapper = JsonMapper.builder().build();

  @Inject protected StargateRequestInfo stargateRequestInfo;

  protected StargateBridge bridge;

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
    String timestamp = "_" + System.currentTimeMillis();

    int len = testKeyspacePrefix.length() + testName.length() + timestamp.length();

    // Alas, may well hit the keyspace name limit so:
    if (len > 48) {
      // need to truncate testName, NOT timestamp, so:
      testName = testName.substring(0, testName.length() - (len - 48));
    }
    testKeyspaceName = testKeyspacePrefix + testName + timestamp;
    testTableName = testTablePrefix + testName + timestamp;

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
  // Schema initialization, CQL Access
  /////////////////////////////////////////////////////////////////////////
   */

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
  // Endpoint construction
  /////////////////////////////////////////////////////////////////////////
   */

  protected String endpointPathForTables(String ksName) {
    return String.format("/v2/schemas/keyspaces/%s/tables", ksName);
  }

  protected String endpointPathForTable(String ksName, String tableName) {
    return String.format("/v2/schemas/keyspaces/%s/tables/%s", ksName, tableName);
  }

  protected String endpointPathForRowAdd(String ksName, String tableName) {
    return String.format("/v2/keyspaces/%s/%s", ksName, tableName);
  }

  protected String endpointPathForRowByPK(String ksName, String tableName, String... primaryKeys) {
    StringBuilder sb = new StringBuilder(String.format("/v2/keyspaces/%s/%s", ksName, tableName));
    for (String key : primaryKeys) {
      sb.append('/').append(key);
    }
    return sb.toString();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // JSON handling, generic
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

  protected <T> T readJsonAs(String body, TypeReference asType) {
    try {
      return (T) objectMapper.readValue(body, asType);
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

  /*
  /////////////////////////////////////////////////////////////////////////
  // JSON helper classes
  /////////////////////////////////////////////////////////////////////////
   */

  protected static class ListOfMapsGetResponseWrapper
      extends Sgv2GetResponse<List<Map<String, Object>>> {
    public ListOfMapsGetResponseWrapper() {
      super(-1, null, null);
    }
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Keyspace creation
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

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Table CRUD
  /////////////////////////////////////////////////////////////////////////
   */

  protected TableNameResponse createSimpleTestTable(String keyspaceName, String tableName) {
    final Sgv2TableAddRequest tableAdd = new Sgv2TableAddRequest(tableName);
    tableAdd.setColumnDefinitions(
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "uuid", false),
            new Sgv2ColumnDefinition("lastName", "text", false),
            new Sgv2ColumnDefinition("firstName", "text", false),
            new Sgv2ColumnDefinition("age", "int", false)));

    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("id"));
    tableAdd.setPrimaryKey(primaryKey);

    return createTable(keyspaceName, tableAdd);
  }

  protected TableNameResponse createComplexTestTable(String keyspaceName, String tableName) {
    final Sgv2TableAddRequest tableAdd = new Sgv2TableAddRequest(tableName);
    tableAdd.setColumnDefinitions(
        Arrays.asList(
            new Sgv2ColumnDefinition("pk0", "uuid", false),
            new Sgv2ColumnDefinition("col1", "frozen<map<date, text>>", false),
            new Sgv2ColumnDefinition("col2", "frozen<set<boolean>>", false),
            new Sgv2ColumnDefinition("col3", "tuple<duration, inet>", false)));

    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("pk0"));
    tableAdd.setPrimaryKey(primaryKey);

    return createTable(keyspaceName, tableAdd);
  }

  protected TableNameResponse createTestTable(
      String keyspaceName,
      String tableName,
      List<String> columns,
      List<String> partitionKey,
      List<String> clusteringKey) {
    Sgv2TableAddRequest tableAdd = new Sgv2TableAddRequest(tableName);

    List<Sgv2ColumnDefinition> columnDefinitions =
        columns.stream()
            .map(x -> x.split(" "))
            .map(y -> new Sgv2ColumnDefinition(y[0], y[1], false))
            .collect(Collectors.toList());
    tableAdd.setColumnDefinitions(columnDefinitions);

    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey();
    primaryKey.setPartitionKey(partitionKey);
    if (clusteringKey != null) {
      primaryKey.setClusteringKey(clusteringKey);
    }
    tableAdd.setPrimaryKey(primaryKey);
    return createTable(keyspaceName, tableAdd);
  }

  protected TableNameResponse createTable(String keyspaceName, Sgv2TableAddRequest addRequest) {
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .contentType(ContentType.JSON)
            .body(asJsonString(addRequest))
            .when()
            .post(endpointPathForTables(keyspaceName))
            .then()
            .statusCode(HttpStatus.SC_CREATED)
            .extract()
            .asString();
    return readJsonAs(response, TableNameResponse.class);
  }

  protected Sgv2Table findTable(String keyspaceName, String tableName) {
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("raw", "true")
            .when()
            .get(endpointPathForTable(keyspaceName, tableName))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAs(response, Sgv2Table.class);
  }

  protected static class TableNameResponse {
    public String name;
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Rows CRUD
  /////////////////////////////////////////////////////////////////////////
   */

  protected String insertRow(String keyspaceName, String tableName, Map<?, ?> row) {
    return insertRowExpectStatus(keyspaceName, tableName, row, HttpStatus.SC_CREATED);
  }

  protected String insertRowExpectStatus(
      String keyspaceName, String tableName, Map<?, ?> row, int expectedStatus) {
    return given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
        .contentType(ContentType.JSON)
        .body(asJsonString(row))
        .when()
        .post(endpointPathForRowAdd(keyspaceName, tableName))
        .then()
        .statusCode(expectedStatus)
        .extract()
        .asString();
  }

  protected List<Map<String, Object>> findRowsAsList(
      String keyspaceName, String tableName, String... primaryKeys) {
    final String path = endpointPathForRowByPK(keyspaceName, tableName, primaryKeys);
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("raw", "true")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    List<Map<String, Object>> data =
        readJsonAs(response, new TypeReference<List<Map<String, Object>>>() {});
    return data;
  }

  protected ArrayNode findRowsAsJsonNode(
      String keyspaceName, String tableName, String... primaryKeys) {
    final String path = endpointPathForRowByPK(keyspaceName, tableName, primaryKeys);
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("raw", "true")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAs(response, ArrayNode.class);
  }
}
