package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.cql.builder.CollectionIndexingType;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.restapi.service.models.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.*;

/**
 * Serves as the base class for integration tests that need to create namespace prior to running the
 * tests and also provides shared convenience methods
 *
 * <p>Note that due to the way how RestAssured is configured in Quarkus tests, we are doing the
 * initialization as first tests to be run. The {@link BeforeAll} annotation can not be used, as
 * rest assured is not configured yet. See https://github.com/quarkusio/quarkus/issues/7690 for more
 * info.
 */
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class RestApiV2QIntegrationTestBase {
  public enum KeyspaceCreation {
    PER_METHOD,
    PER_CLASS,
    NONE
  }

  protected static final ObjectMapper objectMapper = JsonMapper.builder().build();

  protected static final TypeReference LIST_OF_MAPS_TYPE =
      new TypeReference<List<Map<String, Object>>>() {};

  private final KeyspaceCreation keyspaceCreation;

  private final String testKeyspacePrefix;

  private final String testTablePrefix;

  private String testKeyspaceName;

  private String testTableName;

  /*
  /////////////////////////////////////////////////////////////////////////
  // Initialization
  /////////////////////////////////////////////////////////////////////////
   */

  protected RestApiV2QIntegrationTestBase(
      String keyspacePrefix, String tablePrefix, KeyspaceCreation keyspaceCreation) {
    this.testKeyspacePrefix = keyspacePrefix;
    this.testTablePrefix = tablePrefix;
    this.keyspaceCreation = keyspaceCreation;
  }

  @BeforeAll
  public void init() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @BeforeEach
  public void initPerTest(TestInfo testInfo) {
    // Let's force lower-case keyspace and table names for defaults; case-sensitive testing
    // needs to use explicitly different values
    String testName = testInfo.getTestMethod().map(ti -> ti.getName()).get().toLowerCase();
    String timestamp = "_" + System.currentTimeMillis();

    int len = testKeyspacePrefix.length() + testName.length() + timestamp.length();

    // Alas, may well hit the keyspace name limit so:
    if (len > 48) {
      // need to truncate testName, NOT timestamp, so:
      testName = testName.substring(0, testName.length() - (len - 48));
    }
    testTableName = testTablePrefix + testName + timestamp;

    // May need automatic keyspace creation on per-class or per-method  basis
    if (keyspaceCreation == KeyspaceCreation.PER_METHOD) {
      testKeyspaceName = testKeyspacePrefix + testName + timestamp;
      createKeyspace(testKeyspaceName);
    }
  }

  // Per-class initialization needs to run "as a test method" before real tests
  @Test
  @Order(Integer.MIN_VALUE)
  public void initPerClass() {
    if (keyspaceCreation == KeyspaceCreation.PER_CLASS) {
      // If shared for test methods create first time it is actually needed
      testKeyspaceName = testKeyspacePrefix + "shared";
      createKeyspace(testKeyspaceName);
    }
  }

  @AfterEach
  public void cleanupPerTest() {
    if (keyspaceCreation == KeyspaceCreation.PER_METHOD) {
      deleteKeyspace(testKeyspaceName);
    }
  }

  // Per-class cleanup needs to run "as a test method" after all real tests
  @Test
  @Order(Integer.MAX_VALUE)
  public void cleanupPerClass() {
    if (keyspaceCreation == KeyspaceCreation.PER_CLASS) {
      deleteKeyspace(testKeyspaceName);
    }
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Accessors
  /////////////////////////////////////////////////////////////////////////
   */

  public String testKeyspaceName() {
    return Objects.requireNonNull(testKeyspaceName, "'testKeyspaceName' not initialized");
  }

  public String testTableName() {
    return Objects.requireNonNull(testTableName, "'testTableName' not initialized");
  }

  protected RequestSpecification givenWithAuth() {
    return givenWithAuthToken(IntegrationTestUtils.getAuthToken());
  }

  protected RequestSpecification givenWithoutAuth() {
    return given();
  }

  protected RequestSpecification givenWithAuthToken(String authTokenValue) {
    return given().header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, authTokenValue);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Endpoint construction
  /////////////////////////////////////////////////////////////////////////
   */

  protected String endpointPathForCQL() {
    return "/v2/cql";
  }

  protected String endpointPathForAllKeyspaces() {
    return "/v2/schemas/keyspaces";
  }

  protected String endpointPathForTables(String ksName) {
    return String.format("/v2/schemas/keyspaces/%s/tables", ksName);
  }

  protected String endpointPathForTable(String ksName, String tableName) {
    return String.format("/v2/schemas/keyspaces/%s/tables/%s", ksName, tableName);
  }

  protected String endpointPathForRowAdd(String ksName, String tableName) {
    return String.format("/v2/keyspaces/%s/%s", ksName, tableName);
  }

  protected String endpointPathForIndexAdd(String ksName, String tableName) {
    return String.format("/v2/schemas/keyspaces/%s/tables/%s/indexes", ksName, tableName);
  }

  protected String endpointPathForUDTAdd(String ksName) {
    return String.format("/v2/schemas/keyspaces/%s/types", ksName);
  }

  protected String endpointPathForAllRows(String ksName, String tableName) {
    return String.format("/v2/keyspaces/%s/%s/rows", ksName, tableName);
  }

  protected String endpointPathForRowGetWith(String ksName, String tableName) {
    return String.format("/v2/keyspaces/%s/%s", ksName, tableName);
  }

  protected String endpointPathForRowByPK(String ksName, String tableName, Object... primaryKeys) {
    StringBuilder sb = new StringBuilder(String.format("/v2/keyspaces/%s/%s", ksName, tableName));
    for (Object key : primaryKeys) {
      // NOTE! Does NOT URL-encode the key -- doing so would lead to double-escaping. But
      // also means this should NOT be used for keys that need escaping
      sb.append('/').append(key);
    }
    return sb.toString();
  }

  // Alternative method that will create template to use for passing segments of primary key
  // that may require escaping (contain slashes, ampersands etc)
  protected String endpointTemplateForRowByPK(String ksName, String tableName, int pkCount) {
    StringBuilder sb = new StringBuilder(String.format("/v2/keyspaces/%s/%s", ksName, tableName));
    for (int i = 0; i < pkCount; ++i) {
      sb.append("/{pk").append(i).append('}');
    }
    return sb.toString();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Map construction
  /////////////////////////////////////////////////////////////////////////
   */

  protected Map<String, Object> map(String key, Object value) {
    return Collections.singletonMap(key, value);
  }

  protected List<Object> list(Object entry) {
    return Collections.singletonList(entry);
  }

  protected Map<String, Object> map(String key1, Object value1, String key2, Object value2) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(key1, value1);
    map.put(key2, value2);
    return map;
  }

  protected Map<String, Object> map(
      String key1, Object value1, String key2, Object value2, String key3, Object value3) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(key1, value1);
    map.put(key2, value2);
    map.put(key3, value3);
    return map;
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
      return wrapped.data();
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

  protected JsonNode readJsonAsTree(String body) {
    try {
      return objectMapper.readTree(body);
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

  protected record ListOfMapsGetResponseWrapper(
      int count, String pageState, List<Map<String, Object>> data) {
    public ListOfMapsGetResponseWrapper() {
      this(-1, null, null);
    }
  }

  protected static class NameResponse {
    public String name;
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Keyspace creation/deletion
  /////////////////////////////////////////////////////////////////////////
   */

  protected void createKeyspace(String keyspaceName) {
    // We are essentially doing this:
    // String cql =
    //    "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH replication = {'class': 'SimpleStrategy',
    // 'replication_factor': 1}"
    //        .formatted(keyspaceName);
    //
    // but use REST API itself to avoid having bootstrap CQL or Bridge client

    String createKeyspace = String.format("{\"name\": \"%s\", \"replicas\": 1}", keyspaceName);
    givenWithAuth()
        .contentType(ContentType.JSON)
        .body(createKeyspace)
        .when()
        .post(endpointPathForAllKeyspaces())
        .then()
        .statusCode(HttpStatus.SC_CREATED);
  }

  protected void deleteKeyspace(String keyspaceName) {
    givenWithAuth()
        .when()
        .delete(endpointPathForAllKeyspaces() + "/{keyspace-id}", keyspaceName)
        .then()
        .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Table Creation, Access
  /////////////////////////////////////////////////////////////////////////
   */

  protected NameResponse createSimpleTestTable(String keyspaceName, String tableName) {
    List<Sgv2ColumnDefinition> columnDefs =
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "uuid", false),
            new Sgv2ColumnDefinition("lastName", "text", false),
            new Sgv2ColumnDefinition("firstName", "text", false),
            new Sgv2ColumnDefinition("age", "int", false));

    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey(Arrays.asList("id"));
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);
    return createTable(keyspaceName, tableAdd);
  }

  protected NameResponse createComplexTestTable(String keyspaceName, String tableName) {
    List<Sgv2ColumnDefinition> columnDefs =
        Arrays.asList(
            new Sgv2ColumnDefinition("pk0", "uuid", false),
            new Sgv2ColumnDefinition("col1", "frozen<map<date, text>>", false),
            new Sgv2ColumnDefinition("col2", "frozen<set<boolean>>", false),
            new Sgv2ColumnDefinition("col3", "tuple<duration, inet>", false));

    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey(Arrays.asList("pk0"));
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);

    return createTable(keyspaceName, tableAdd);
  }

  protected NameResponse createTestTable(
      String keyspaceName,
      String tableName,
      List<String> columns,
      List<String> partitionKey,
      List<String> clusteringKey) {
    List<Sgv2ColumnDefinition> columnDefs =
        columns.stream()
            .map(x -> x.split(" "))
            .map(y -> new Sgv2ColumnDefinition(y[0], y[1], false))
            .collect(Collectors.toList());
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey(partitionKey, clusteringKey);
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);
    return createTable(keyspaceName, tableAdd);
  }

  protected NameResponse createTable(String keyspaceName, Sgv2TableAddRequest addRequest) {
    String response = tryCreateTable(keyspaceName, addRequest, HttpStatus.SC_CREATED);
    return readJsonAs(response, NameResponse.class);
  }

  protected String tryCreateTable(
      String keyspaceName, Sgv2TableAddRequest addRequest, int expStatus) {
    return givenWithAuth()
        .contentType(ContentType.JSON)
        .body(asJsonString(addRequest))
        .when()
        .post(endpointPathForTables(keyspaceName))
        .then()
        .statusCode(expStatus)
        .extract()
        .asString();
  }

  protected Sgv2Table findTable(String keyspaceName, String tableName) {
    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(endpointPathForTable(keyspaceName, tableName))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAs(response, Sgv2Table.class);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Index creation
  /////////////////////////////////////////////////////////////////////////
   */

  protected void createTestIndex(
      String keyspaceName,
      String tableName,
      String columnName,
      String indexName,
      boolean ifNotExists,
      CollectionIndexingType kind) {
    Sgv2IndexAddRequest indexAdd = new Sgv2IndexAddRequest(columnName, indexName);
    indexAdd.setIfNotExists(ifNotExists);
    indexAdd.setKind(kind);

    String response = tryCreateIndex(keyspaceName, tableName, indexAdd, HttpStatus.SC_CREATED);
    IndexResponse successResponse = readJsonAs(response, IndexResponse.class);
    assertThat(successResponse.success).isTrue();
  }

  protected static class IndexResponse {
    public Boolean success;
  }

  protected String tryCreateIndex(
      String keyspaceName, String tableName, Sgv2IndexAddRequest indexAdd, int expectedResult) {
    return givenWithAuth()
        .contentType(ContentType.JSON)
        .body(asJsonString(indexAdd))
        .when()
        .post(endpointPathForIndexAdd(keyspaceName, tableName))
        .then()
        .statusCode(expectedResult)
        .extract()
        .asString();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for Rows CRUD
  /////////////////////////////////////////////////////////////////////////
   */

  protected List<Map<String, String>> insertRows(
      String keyspaceName, String tableName, List<List<String>> rows) {
    return insertRowsFromListOfList(keyspaceName, tableName, rows, null);
  }

  protected List<Map<String, String>> insertRows(
      String keyspaceName, String tableName, List<List<String>> rows, Boolean compactMapData) {
    return insertRowsFromListOfList(keyspaceName, tableName, rows, compactMapData);
  }

  private List<Map<String, String>> insertRowsFromListOfList(
      String keyspaceName, String tableName, List<List<String>> rows, Boolean compactMapData) {
    final List<Map<String, String>> insertedRows = new ArrayList<>();
    for (List<String> row : rows) {
      Map<String, String> rowMap = new HashMap<>();
      for (String kv : row) {
        // Split on first space, leave others in (with no limit we'd silently
        // drop later space-separated parts)
        String[] parts = kv.split(" ", 2);
        rowMap.put(parts[0].trim(), parts[1].trim());
      }
      insertRow(keyspaceName, tableName, rowMap, compactMapData);
      insertedRows.add(rowMap);
    }

    return insertedRows;
  }

  protected List<Map<String, Object>> insertTypedRows(
      String keyspaceName, String tableName, List<Map<String, Object>> rows) {
    return insertTypedRowsInternal(keyspaceName, tableName, rows, null);
  }

  protected List<Map<String, Object>> insertTypedRows(
      String keyspaceName,
      String tableName,
      List<Map<String, Object>> rows,
      Boolean compactMapData) {
    return insertTypedRowsInternal(keyspaceName, tableName, rows, compactMapData);
  }

  private List<Map<String, Object>> insertTypedRowsInternal(
      String keyspaceName,
      String tableName,
      List<Map<String, Object>> rows,
      Boolean compactMapData) {
    final List<Map<String, Object>> insertedRows = new ArrayList<>();
    for (Map<String, Object> row : rows) {
      insertRow(keyspaceName, tableName, row, compactMapData);
      insertedRows.add(row);
    }
    return insertedRows;
  }

  protected String insertRow(String keyspaceName, String tableName, Map<?, ?> row) {
    return insertRowExpectStatus(keyspaceName, tableName, row, HttpStatus.SC_CREATED);
  }

  protected String insertRow(
      String keyspaceName, String tableName, Map<?, ?> row, Boolean compactMapData) {
    return insertRowWithCompactMapFlagExpectStatus(
        keyspaceName, tableName, row, HttpStatus.SC_CREATED, compactMapData);
  }

  protected String insertRowWithCompactMapFlagExpectStatus(
      String keyspaceName,
      String tableName,
      Map<?, ?> row,
      int expectedStatus,
      Boolean compactMapFlag) {
    return getInsertRowRequest(row, compactMapFlag)
        .when()
        .post(endpointPathForRowAdd(keyspaceName, tableName))
        .then()
        .statusCode(expectedStatus)
        .extract()
        .asString();
  }

  private RequestSpecification getInsertRowRequest(Map<?, ?> row, Boolean compactMapFlag) {
    RequestSpecification requestSpecification =
        givenWithAuth().contentType(ContentType.JSON).body(asJsonString(row));
    return compactMapFlag == null
        ? requestSpecification
        : requestSpecification.queryParam("compactMapData", compactMapFlag);
  }

  protected String insertRowExpectStatus(
      String keyspaceName, String tableName, Map<?, ?> row, int expectedStatus) {
    return getInsertRowRequest(row, null)
        .when()
        .post(endpointPathForRowAdd(keyspaceName, tableName))
        .then()
        .statusCode(expectedStatus)
        .extract()
        .asString();
  }

  protected List<Map<String, Object>> findAllRowsAsList(String keyspaceName, String tableName) {
    final String path = endpointPathForAllRows(keyspaceName, tableName);
    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAs(response, LIST_OF_MAPS_TYPE);
  }

  protected JsonNode findRowsAsJsonNode(
      String keyspaceName, String tableName, Boolean compactMapData, Object... primaryKeys) {
    final String path = endpointPathForRowByPK(keyspaceName, tableName, primaryKeys);
    RequestSpecification request = givenWithAuth();
    if (compactMapData != null) {
      request = request.queryParam("compactMapData", compactMapData);
    }
    String response =
        request
            .queryParam("raw", "true")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAsTree(response);
  }

  protected List<Map<String, Object>> findRowsAsList(
      String keyspaceName, String tableName, Object... primaryKeys) {
    final String path = endpointPathForRowByPK(keyspaceName, tableName, primaryKeys);
    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAs(response, LIST_OF_MAPS_TYPE);
  }

  protected ListOfMapsGetResponseWrapper findRowsAsWrapped(
      String keyspaceName, String tableName, Object... primaryKeys) {
    final String path = endpointPathForRowByPK(keyspaceName, tableName, primaryKeys);
    String response =
        givenWithAuth()
            .queryParam("raw", "false")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAs(response, ListOfMapsGetResponseWrapper.class);
  }

  protected ArrayNode findRowsAsJsonNode(
      String keyspaceName, String tableName, Object... primaryKeys) {
    final String path = endpointPathForRowByPK(keyspaceName, tableName, primaryKeys);
    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAs(response, ArrayNode.class);
  }

  protected ArrayNode findRowsWithWhereAsJsonNode(
      String keyspaceName, String tableName, String whereClause) {
    return findRowsWithWhereAsJsonNodeInternal(keyspaceName, tableName, whereClause, null);
  }

  protected ArrayNode findRowsWithWhereAsJsonNode(
      String keyspaceName, String tableName, String whereClause, Boolean compactMapData) {
    return findRowsWithWhereAsJsonNodeInternal(
        keyspaceName, tableName, whereClause, compactMapData);
  }

  private ArrayNode findRowsWithWhereAsJsonNodeInternal(
      String keyspaceName, String tableName, String whereClause, Boolean compactMapData) {
    RequestSpecification request =
        givenWithAuth().queryParam("raw", true).queryParam("where", whereClause);
    if (compactMapData != null) {
      request = request.queryParam("compactMapData", compactMapData);
    }
    String response =
        request
            .when()
            .get(endpointPathForRowGetWith(testKeyspaceName(), tableName))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return (ArrayNode) readJsonAsTree(response);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for other test setup
  /////////////////////////////////////////////////////////////////////////
   */

  /** @return Partition key of the first row */
  protected Integer setupClusteringTestCase(String keyspaceName, String tableName) {
    List<Sgv2ColumnDefinition> columnDefs =
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "int", false),
            new Sgv2ColumnDefinition("lastName", "text", false),
            new Sgv2ColumnDefinition("firstName", "text", false),
            new Sgv2ColumnDefinition("age", "int", true),
            new Sgv2ColumnDefinition("expense_id", "int", false));
    Sgv2Table.PrimaryKey primaryKey =
        new Sgv2Table.PrimaryKey(Arrays.asList("id"), Arrays.asList("expense_id"));
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);
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
    List<Sgv2ColumnDefinition> columnDefs =
        Arrays.asList(
            new Sgv2ColumnDefinition("pk0", "int", false),
            new Sgv2ColumnDefinition("pk1", "text", false),
            new Sgv2ColumnDefinition("pk2", "int", false),
            new Sgv2ColumnDefinition("ck0", "int", false),
            new Sgv2ColumnDefinition("ck1", "text", false),
            new Sgv2ColumnDefinition("v", "int", false));
    Sgv2Table.PrimaryKey primaryKey =
        new Sgv2Table.PrimaryKey(Arrays.asList("pk0", "pk1", "pk2"), Arrays.asList("ck0", "ck1"));
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);
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

  protected String updateRowReturnResponse(
      String updatePath, boolean raw, Map<?, ?> payload, Boolean compactMapData) {
    return updateRowReturnResponse(updatePath, raw, payload, HttpStatus.SC_OK, compactMapData);
  }

  protected String updateRowReturnResponse(String updatePath, boolean raw, Map<?, ?> payload) {
    return updateRowReturnResponse(updatePath, raw, payload, HttpStatus.SC_OK);
  }

  protected String updateRowReturnResponse(
      String updatePath,
      boolean raw,
      Map<?, ?> payloadMap,
      int expectedStatus,
      Boolean compactMapData) {
    return updateRowReturnResponse(
        updatePath, raw, asJsonString(payloadMap), expectedStatus, compactMapData);
  }

  protected String updateRowReturnResponse(
      String updatePath, boolean raw, Map<?, ?> payloadMap, int expectedStatus) {
    return updateRowReturnResponse(updatePath, raw, asJsonString(payloadMap), expectedStatus);
  }

  private RequestSpecification getUpdateRequest(String payloadJSON, boolean raw) {
    return givenWithAuth().queryParam("raw", raw).contentType(ContentType.JSON).body(payloadJSON);
  }

  protected String updateRowReturnResponse(
      String updatePath,
      boolean raw,
      String payloadJSON,
      int expectedStatus,
      Boolean compactMapData) {
    RequestSpecification req = getUpdateRequest(payloadJSON, raw);
    if (compactMapData != null) {
      req.queryParam("compactMapData", compactMapData);
    }
    return req.when().put(updatePath).then().statusCode(expectedStatus).extract().asString();
  }

  protected String updateRowReturnResponse(
      String updatePath, boolean raw, String payloadJSON, int expectedStatus) {
    return getUpdateRequest(payloadJSON, raw)
        .when()
        .put(updatePath)
        .then()
        .statusCode(expectedStatus)
        .extract()
        .asString();
  }

  protected String patchRowReturnResponse(
      String patchPath, boolean raw, Map<?, ?> payload, Boolean compactMapData) {
    return patchRowReturnResponse(patchPath, raw, payload, HttpStatus.SC_OK, compactMapData);
  }

  protected String patchRowReturnResponse(String patchPath, boolean raw, Map<?, ?> payload) {
    return patchRowReturnResponse(patchPath, raw, payload, HttpStatus.SC_OK);
  }

  protected String patchRowReturnResponse(
      String patchPath, boolean raw, Map<?, ?> payloadMap, int expectedStatus) {
    return patchRowReturnResponse(patchPath, raw, asJsonString(payloadMap), expectedStatus);
  }

  protected String patchRowReturnResponse(
      String patchPath,
      boolean raw,
      Map<?, ?> payloadMap,
      int expectedStatus,
      Boolean compactMapData) {
    return patchRowReturnResponse(
        patchPath, raw, asJsonString(payloadMap), expectedStatus, compactMapData);
  }

  protected String patchRowReturnResponse(
      String patchPath,
      boolean raw,
      String payloadJSON,
      int expectedStatus,
      Boolean compactMapData) {
    RequestSpecification req = getPatchRequest(payloadJSON, raw);
    if (compactMapData != null) {
      req.queryParam("compactMapData", compactMapData);
    }
    return req.when().patch(patchPath).then().statusCode(expectedStatus).extract().asString();
  }

  private RequestSpecification getPatchRequest(String payloadJSON, boolean raw) {
    return givenWithAuth().queryParam("raw", raw).contentType(ContentType.JSON).body(payloadJSON);
  }

  protected String patchRowReturnResponse(
      String patchPath, boolean raw, String payloadJSON, int expectedStatus) {
    return getPatchRequest(payloadJSON, raw)
        .when()
        .patch(patchPath)
        .then()
        .statusCode(expectedStatus)
        .extract()
        .asString();
  }

  protected List<RestApiV2QSchemaIndexesIT.IndexDesc> findAllIndexesViaEndpoint(
      String ksName, String tableName) {
    String response = getAllIndexes(ksName, tableName, null);
    return Arrays.asList(readJsonAs(response, RestApiV2QSchemaIndexesIT.IndexDesc[].class));
  }

  protected String getAllIndexes(String ksName, String tableName, Boolean compactMapData) {
    RequestSpecification req = givenWithAuth();
    if (compactMapData != null) {
      req.queryParam("compactMapData", compactMapData);
    }
    return req.when()
        .get(endpointPathForAllIndexes(ksName, tableName))
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract()
        .asString();
  }

  protected String endpointPathForAllIndexes(String ksName, String tableName) {
    return String.format("/v2/schemas/keyspaces/%s/tables/%s/indexes", ksName, tableName);
  }
}
