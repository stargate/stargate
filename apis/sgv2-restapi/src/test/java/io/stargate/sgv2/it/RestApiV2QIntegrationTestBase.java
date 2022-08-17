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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

  protected String endpointPathForAllRows(String ksName, String tableName) {
    return String.format("/v2/keyspaces/%s/%s/rows", ksName, tableName);
  }

  protected String endpointPathForRowAdd(String ksName, String tableName) {
    return String.format("/v2/keyspaces/%s/%s", ksName, tableName);
  }

  protected String endpointPathForRowByPK(String ksName, String tableName, Object... primaryKeys) {
    StringBuilder sb = new StringBuilder(String.format("/v2/keyspaces/%s/%s", ksName, tableName));
    for (Object key : primaryKeys) {
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
  // Helper methods for Table Creation, Access
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

  protected List<Map<String, String>> insertRows(
      String keyspaceName, String tableName, List<List<String>> rows) {
    final List<Map<String, String>> insertedRows = new ArrayList<>();
    for (List<String> row : rows) {
      Map<String, String> rowMap = new HashMap<>();
      for (String kv : row) {
        // Split on first space, leave others in (with no limit we'd silently
        // drop later space-separated parts)
        String[] parts = kv.split(" ", 2);
        rowMap.put(parts[0].trim(), parts[1].trim());
      }
      insertRow(keyspaceName, tableName, rowMap);
      insertedRows.add(rowMap);
    }

    return insertedRows;
  }

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
      String keyspaceName, String tableName, Object... primaryKeys) {
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
      String keyspaceName, String tableName, Object... primaryKeys) {
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

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for other test setup
  /////////////////////////////////////////////////////////////////////////
   */

  /** @return Partition key of the first row */
  protected Integer setupClusteringTestCase(String keyspaceName, String tableName) {
    // createTestTableWithClustering
    final Sgv2TableAddRequest tableAdd = new Sgv2TableAddRequest(tableName);
    tableAdd.setColumnDefinitions(
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "int", false),
            new Sgv2ColumnDefinition("lastName", "text", false),
            new Sgv2ColumnDefinition("firstName", "text", false),
            new Sgv2ColumnDefinition("age", "int", true),
            new Sgv2ColumnDefinition("expense_id", "int", false)));
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("id"));
    primaryKey.setClusteringKey(Arrays.asList("expense_id"));
    tableAdd.setPrimaryKey(primaryKey);
    createTable(keyspaceName, tableAdd);

    final Integer firstRowId = 1;
    Map<String, Object> row = new HashMap<>();
    row.put("id", firstRowId);
    row.put("firstName", "John");
    row.put("expense_id", 1);
    insertRow(keyspaceName, tableName, row);

    row = new HashMap<>();
    row.put("id", firstRowId);
    row.put("firstName", "John");
    row.put("expense_id", 2);
    insertRow(keyspaceName, tableName, row);

    row = new HashMap<>();
    row.put("id", 2);
    row.put("firstName", "Jane");
    row.put("expense_id", 1);
    insertRow(keyspaceName, tableName, row);

    // Duplicate, will only try to update 3rd entry
    row = new HashMap<>();
    row.put("id", 2);
    row.put("firstName", "Jane");
    row.put("expense_id", 1);
    insertRow(keyspaceName, tableName, row);

    return firstRowId;
  }

  protected void setupMixedClusteringTestCase(String keyspaceName, String tableName) {
    // createTestTableWithMixedClustering(keyspaceName, tableName)
    final Sgv2TableAddRequest tableAdd = new Sgv2TableAddRequest(tableName);
    tableAdd.setColumnDefinitions(
        Arrays.asList(
            new Sgv2ColumnDefinition("pk0", "int", false),
            new Sgv2ColumnDefinition("pk1", "text", false),
            new Sgv2ColumnDefinition("pk2", "int", false),
            new Sgv2ColumnDefinition("ck0", "int", false),
            new Sgv2ColumnDefinition("ck1", "text", false),
            new Sgv2ColumnDefinition("v", "int", false)));
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("pk0", "pk1", "pk2"));
    primaryKey.setClusteringKey(Arrays.asList("ck0", "ck1"));
    tableAdd.setPrimaryKey(primaryKey);
    createTable(keyspaceName, tableAdd);

    Map<String, Object> row = new HashMap<>();
    row.put("pk0", 1);
    row.put("pk1", "one");
    row.put("pk2", -1);
    row.put("ck0", 10);
    row.put("ck1", "foo");
    row.put("v", 9);
    insertRow(keyspaceName, tableName, row);

    row = new HashMap<>();
    row.put("pk0", 1);
    row.put("pk1", "one");
    row.put("pk2", -1);
    row.put("ck0", 20);
    row.put("ck1", "foo");
    row.put("v", 19);
    insertRow(keyspaceName, tableName, row);

    row = new HashMap<>();
    row.put("pk0", 2);
    row.put("pk1", "two");
    row.put("pk2", -2);
    row.put("ck0", 10);
    row.put("ck1", "bar");
    row.put("v", 18);
    insertRow(keyspaceName, tableName, row);
  }
}
