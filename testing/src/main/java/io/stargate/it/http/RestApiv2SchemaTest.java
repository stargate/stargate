package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.web.models.Keyspace;
import io.stargate.web.restapi.models.ColumnDefinition;
import io.stargate.web.restapi.models.GetResponseWrapper;
import io.stargate.web.restapi.models.PrimaryKey;
import io.stargate.web.restapi.models.RESTResponseWrapper;
import io.stargate.web.restapi.models.TableAdd;
import io.stargate.web.restapi.models.TableOptions;
import io.stargate.web.restapi.models.TableResponse;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
public class RestApiv2SchemaTest extends BaseIntegrationTest {
  private String keyspaceName;
  private String tableName;
  private static String authToken;
  private String host;
  private String restUrlBase;

  static class ListOfMapsGetResponseWrapper extends GetResponseWrapper<List<Map<String, Object>>> {
    public ListOfMapsGetResponseWrapper() {
      super(-1, null, null);
    }
  }

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  public void setup(TestInfo testInfo, StargateConnectionInfo cluster) throws IOException {
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
  /* Test methods for Table CRUD operations
  /************************************************************************
   */

  @Test
  public void tableCreateSimple() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s?raw=true",
                restUrlBase, keyspaceName, tableName),
            HttpStatus.SC_OK);

    TableResponse table = objectMapper.readValue(body, TableResponse.class);
    assertThat(table.getKeyspace()).isEqualTo(keyspaceName);
    assertThat(table.getName()).isEqualTo(tableName);
    assertThat(table.getColumnDefinitions()).isNotNull();
  }

  @Test
  public void tableCreateWithNullOptions() throws IOException {
    createTestKeyspace(keyspaceName);

    TableAdd tableAdd = new TableAdd();
    tableAdd.setName("t1");

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("id", "uuid"));
    columnDefinitions.add(new ColumnDefinition("lastName", "text"));
    columnDefinitions.add(new ColumnDefinition("firstName", "text"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("id"));
    tableAdd.setPrimaryKey(primaryKey);
    tableAdd.setTableOptions(null);

    String body =
            RestUtils.post(
                    authToken,
                    String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
                    objectMapper.writeValueAsString(tableAdd),
                    HttpStatus.SC_CREATED);

    RestApiv2Test.NameResponse response = objectMapper.readValue(body, RestApiv2Test.NameResponse.class);
    assertThat(response.name).isEqualTo(tableAdd.getName());
  }

  @Test
  public void tableCreateWithMissingClustering() throws IOException {
    createTestKeyspace(keyspaceName);
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("pk1", "int"));
    columnDefinitions.add(new ColumnDefinition("ck1", "int"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("pk1"));
    primaryKey.setClusteringKey(Collections.singletonList("ck1"));
    tableAdd.setPrimaryKey(primaryKey);

    tableAdd.setTableOptions(new TableOptions(0, null));

    String body =
            RestUtils.post(
                    authToken,
                    String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
                    objectMapper.writeValueAsString(tableAdd),
                    HttpStatus.SC_CREATED);

    RestApiv2Test.NameResponse response = objectMapper.readValue(body, RestApiv2Test.NameResponse.class);
    assertThat(response.name).isEqualTo(tableAdd.getName());

    body =
            RestUtils.get(
                    authToken,
                    String.format(
                            "%s/v2/schemas/keyspaces/%s/tables/%s?raw=true",
                            restUrlBase, keyspaceName, tableName),
                    HttpStatus.SC_OK);

    TableResponse table = objectMapper.readValue(body, TableResponse.class);
    assertThat(table.getTableOptions().getClusteringExpression().get(0).getOrder())
            .isEqualTo("ASC");
  }

  @Test
  public void tableUpdateSimple() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    TableAdd tableUpdate = new TableAdd();
    tableUpdate.setName(tableName);

    TableOptions tableOptions = new TableOptions();
    tableOptions.setDefaultTimeToLive(5);
    tableUpdate.setTableOptions(tableOptions);

    RestUtils.put(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/tables/%s", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(tableUpdate),
        HttpStatus.SC_OK);
  }

  @Test
  public void tablesGetWrapped() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces/system/tables", restUrlBase),
            HttpStatus.SC_OK);

    TableResponse[] tables = readWrappedRESTResponse(body, TableResponse[].class);

    assertThat(tables.length).isGreaterThan(5);
    assertThat(tables)
        .anySatisfy(
            value ->
                assertThat(value)
                    .isEqualToComparingOnlyGivenFields(
                        new TableResponse("local", "system", null, null, null),
                        "name",
                        "keyspace"));
  }

  @Test
  public void tablesGetRaw() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces/system/tables?raw=true", restUrlBase),
            HttpStatus.SC_OK);

    TableResponse[] tables = objectMapper.readValue(body, TableResponse[].class);

    assertThat(tables.length).isGreaterThan(5);
    assertThat(tables)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .ignoringFields("columnDefinitions", "primaryKey", "tableOptions")
                    .isEqualTo(new TableResponse("local", "system", null, null, null)));
  }

  @Test
  public void tableGetWrapped() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces/system/tables/local", restUrlBase),
            HttpStatus.SC_OK);

    TableResponse table = readWrappedRESTResponse(body, TableResponse.class);
    assertThat(table.getKeyspace()).isEqualTo("system");
    assertThat(table.getName()).isEqualTo("local");
    assertThat(table.getColumnDefinitions()).isNotNull().isNotEmpty();
  }

  @Test
  public void tableGetRaw() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces/system/tables/local?raw=true", restUrlBase),
            HttpStatus.SC_OK);

    TableResponse table = objectMapper.readValue(body, TableResponse.class);
    assertThat(table.getKeyspace()).isEqualTo("system");
    assertThat(table.getName()).isEqualTo("local");
    assertThat(table.getColumnDefinitions()).isNotNull().isNotEmpty();
  }

  @Test
  public void tableGetComplex() throws IOException {
    createTestKeyspace(keyspaceName);
    createComplexTestTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s", restUrlBase, keyspaceName, tableName),
            HttpStatus.SC_OK);

    TableResponse table = readWrappedRESTResponse(body, TableResponse.class);
    assertThat(table.getKeyspace()).isEqualTo(keyspaceName);
    assertThat(table.getName()).isEqualTo(tableName);
    assertThat(table.getColumnDefinitions())
        .hasSize(4)
        .anySatisfy(
            columnDefinition ->
                assertThat(columnDefinition)
                    .usingRecursiveComparison()
                    .isEqualTo(new ColumnDefinition("col1", "frozen<map<date, text>>", false)));
  }

  @Test
  public void tableGetFailNotFound() throws IOException {
    RestUtils.get(
        authToken,
        String.format("%s/v2/schemas/keyspaces/system/tables/tbl_not_found", restUrlBase),
        HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void tableDelete() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    RestUtils.delete(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/tables/%s", restUrlBase, keyspaceName, tableName),
        HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void testMixedCaseTable() throws IOException {
    createTestKeyspace(keyspaceName);

    String tableName = "MixedCaseTable";
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("ID", "uuid"));
    columnDefinitions.add(new ColumnDefinition("Lastname", "text"));
    columnDefinitions.add(new ColumnDefinition("Firstname", "text"));
    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("ID"));
    tableAdd.setPrimaryKey(primaryKey);

    // ensure table name is preserved
    String body =
            RestUtils.post(
                    authToken,
                    String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
                    objectMapper.writeValueAsString(tableAdd),
                    HttpStatus.SC_CREATED);

    TableResponse tableResponse =
            objectMapper.readValue(body, TableResponse.class);
    assertThat(tableResponse.getName()).isEqualTo(tableName);

    // insert a row
    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("ID", rowIdentifier);
    row.put("Firstname", "John");
    row.put("Lastname", "Doe");

    RestUtils.post(
            authToken,
            String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(row),
            HttpStatus.SC_CREATED);

    // retrieve the row by ID and ensure column names are as expected
    String whereClause = String.format("{\"ID\":{\"$eq\":\"%s\"}}", rowIdentifier);
    body =
            RestUtils.get(
                    authToken,
                    String.format(
                            "%s:8082/v2/keyspaces/%s/%s?where=%s", host, keyspaceName, tableName, whereClause),
                    HttpStatus.SC_OK);
    ListOfMapsGetResponseWrapper getResponseWrapper = objectMapper.readValue(body, ListOfMapsGetResponseWrapper.class);
    List<Map<String, Object>> data = getResponseWrapper.getData();
    assertThat(data.get(0).get("ID")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("Firstname")).isEqualTo("John");
    assertThat(data.get(0).get("Lastname")).isEqualTo("Doe");
  }

  /*
  /************************************************************************
  /* Test methods for Index CRUD operations
  /************************************************************************
   */



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

  private void createSimpleTestTable(String keyspaceName, String tableName) {
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("id", "uuid"));
    columnDefinitions.add(new ColumnDefinition("lastName", "text"));
    columnDefinitions.add(new ColumnDefinition("firstName", "text"));
    columnDefinitions.add(new ColumnDefinition("age", "int"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("id"));
    tableAdd.setPrimaryKey(primaryKey);

    try {
      RestUtils.post(
          authToken,
          String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
          objectMapper.writeValueAsString(tableAdd),
          HttpStatus.SC_CREATED);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void createComplexTestTable(String keyspaceName, String tableName) {
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("pk0", "uuid"));
    columnDefinitions.add(new ColumnDefinition("col1", "frozen<map<date, text>>"));
    columnDefinitions.add(new ColumnDefinition("col2", "frozen<set<boolean>>"));
    columnDefinitions.add(new ColumnDefinition("col3", "frozen<tuple<duration, inet>>"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("pk0"));
    tableAdd.setPrimaryKey(primaryKey);

    try {
      RestUtils.post(
          authToken,
          String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
          objectMapper.writeValueAsString(tableAdd),
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
