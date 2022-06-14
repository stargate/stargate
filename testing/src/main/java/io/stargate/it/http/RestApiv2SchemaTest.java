package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.web.models.ApiError;
import io.stargate.web.models.Keyspace;
import io.stargate.web.restapi.models.*;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
@ApiServiceSpec(parametersCustomizer = "buildApiServiceParameters")
public class RestApiv2SchemaTest extends BaseRestApiTest {
  private String keyspaceName;
  private String tableName;
  private static String authToken;
  private String restUrlBase;

  static class ListOfMapsGetResponseWrapper extends GetResponseWrapper<List<Map<String, Object>>> {
    public ListOfMapsGetResponseWrapper() {
      super(-1, null, null);
    }
  }

  static class MapGetResponseWrapper extends GetResponseWrapper<Map<String, Object>> {
    public MapGetResponseWrapper() {
      super(-1, null, null);
    }
  }

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
  /* Test methods for Table CRUD operations
  /************************************************************************
   */

  @Test
  public void tableCreateBasic() throws IOException {
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

    RestApiv2RowsTest.NameResponse response =
        objectMapper.readValue(body, RestApiv2RowsTest.NameResponse.class);
    assertThat(response.name).isEqualTo(tableAdd.getName());
  }

  @Test
  public void tableCreateWithNoClustering() throws IOException {
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

    RestApiv2RowsTest.NameResponse response =
        objectMapper.readValue(body, RestApiv2RowsTest.NameResponse.class);
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
  public void tableCreateWithMultClustering() throws IOException {
    createTestKeyspace(keyspaceName);
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    // Add columns in sort of reverse order wrt primary key columns
    List<ColumnDefinition> columnDefinitions =
        Arrays.asList(
            new ColumnDefinition("value", "int"),
            new ColumnDefinition("ck2", "int"),
            new ColumnDefinition("ck1", "int"),
            new ColumnDefinition("pk2", "int"),
            new ColumnDefinition("pk1", "int"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    // Create partition and clustering keys in order different from that of all-columns
    // definitions
    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("pk1", "pk2"));
    primaryKey.setClusteringKey(Arrays.asList("ck1", "ck2"));
    tableAdd.setPrimaryKey(primaryKey);

    tableAdd.setTableOptions(new TableOptions(0, null));

    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_CREATED);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s?raw=true",
                restUrlBase, keyspaceName, tableName),
            HttpStatus.SC_OK);

    TableResponse table = objectMapper.readValue(body, TableResponse.class);

    // First, verify that partition key ordering is like we expect
    assertThat(table.getTableOptions().getClusteringExpression().size()).isEqualTo(2);
    assertThat(table.getTableOptions().getClusteringExpression().get(0).getColumn())
        .isEqualTo("ck1");
    assertThat(table.getTableOptions().getClusteringExpression().get(1).getColumn())
        .isEqualTo("ck2");

    // And then the same wrt full primary key definition
    PrimaryKey pk = table.getPrimaryKey();
    assertThat(pk.getPartitionKey().size()).isEqualTo(2);
    assertThat(pk.getPartitionKey()).isEqualTo(Arrays.asList("pk1", "pk2"));
    assertThat(pk.getClusteringKey().size()).isEqualTo(2);
    assertThat(pk.getClusteringKey()).isEqualTo(Arrays.asList("ck1", "ck2"));
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
  public void tableWithMixedCaseNames() throws IOException {
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

    TableResponse tableResponse = objectMapper.readValue(body, TableResponse.class);
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
                "%s/v2/keyspaces/%s/%s?where=%s",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);
    ListOfMapsGetResponseWrapper getResponseWrapper =
        objectMapper.readValue(body, ListOfMapsGetResponseWrapper.class);
    List<Map<String, Object>> data = getResponseWrapper.getData();
    assertThat(data.get(0).get("ID")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("Firstname")).isEqualTo("John");
    assertThat(data.get(0).get("Lastname")).isEqualTo("Doe");
  }

  /*
  /************************************************************************
  /* Test methods for Column CRUD operations
  /************************************************************************
   */

  @Test
  public void columnAddBasic() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    ColumnDefinition columnDefinition = new ColumnDefinition("name", "text");

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns",
                restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_CREATED);
    @SuppressWarnings("unchecked")
    Map<String, String> response = objectMapper.readValue(body, Map.class);

    assertThat(response.get("name")).isEqualTo("name");

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true",
                restUrlBase, keyspaceName, tableName, "name"),
            HttpStatus.SC_OK);
    ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
    assertThat(column).usingRecursiveComparison().isEqualTo(columnDefinition);
  }

  @Test
  public void columnAddStatic() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTableWithClustering(keyspaceName, tableName);

    ColumnDefinition columnDefinition = new ColumnDefinition("balance", "float", true);

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns",
                restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_CREATED);
    @SuppressWarnings("unchecked")
    Map<String, String> response = objectMapper.readValue(body, Map.class);

    assertThat(response.get("name")).isEqualTo("balance");

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true",
                restUrlBase, keyspaceName, tableName, "balance"),
            HttpStatus.SC_OK);
    ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
    assertThat(column).usingRecursiveComparison().isEqualTo(columnDefinition);
  }

  @Test
  public void columnAddBadType() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    ColumnDefinition columnDefinition = new ColumnDefinition("name", "badType");

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns",
                restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnUpdate() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    final ColumnDefinition columnDefinition = new ColumnDefinition("identifier", "uuid");
    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, keyspaceName, tableName, "id"),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_OK);
    @SuppressWarnings("unchecked")
    Map<String, String> response = objectMapper.readValue(body, Map.class);
    assertThat(response.get("name")).isEqualTo("identifier");
    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true",
                restUrlBase, keyspaceName, tableName, "identifier"),
            HttpStatus.SC_OK);
    ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
    assertThat(column).usingRecursiveComparison().isEqualTo(columnDefinition);
  }

  @Test
  public void columnUpdateNotFound() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, keyspaceName, tableName, "notFound"),
            objectMapper.writeValueAsString(new ColumnDefinition("name", "text")),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnUpdateBadTable() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, keyspaceName, "foo", "age"),
            objectMapper.writeValueAsString(new ColumnDefinition("name", "text")),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnUpdateBadKeyspace() throws IOException {
    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, "foo", tableName, "age"),
            objectMapper.writeValueAsString(new ColumnDefinition("name", "text")),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnGetWrapped() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, keyspaceName, tableName, "age"),
            HttpStatus.SC_OK);
    ColumnDefinition column = readWrappedRESTResponse(body, ColumnDefinition.class);
    assertThat(column)
        .usingRecursiveComparison()
        .isEqualTo(new ColumnDefinition("age", "int", false));
  }

  @Test
  public void columnGetRaw() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true",
                restUrlBase, keyspaceName, tableName, "age"),
            HttpStatus.SC_OK);
    ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
    assertThat(column)
        .usingRecursiveComparison()
        .isEqualTo(new ColumnDefinition("age", "int", false));
  }

  @Test
  public void columnGetComplex() throws IOException {
    createTestKeyspace(keyspaceName);
    createComplexTestTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, keyspaceName, tableName, "col1"),
            HttpStatus.SC_OK);
    ColumnDefinition column = readWrappedRESTResponse(body, ColumnDefinition.class);
    assertThat(column)
        .usingRecursiveComparison()
        .isEqualTo(new ColumnDefinition("col1", "frozen<map<date, text>>", false));
  }

  @Test
  public void columnGetBadKeyspace() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, "foo", tableName, "age"),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnGetBadTable() throws IOException {
    createTestKeyspace(keyspaceName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, keyspaceName, "foo", "age"),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnGetNotFound() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, keyspaceName, tableName, "foo"),
            HttpStatus.SC_NOT_FOUND);
    ApiError response = objectMapper.readValue(body, ApiError.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnsGetWrapped() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns",
                restUrlBase, keyspaceName, tableName),
            HttpStatus.SC_OK);
    ColumnDefinition[] columns = readWrappedRESTResponse(body, ColumnDefinition[].class);
    assertThat(columns)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .isEqualTo(new ColumnDefinition("id", "uuid", false)));
  }

  @Test
  public void columnsGetRaw() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns?raw=true",
                restUrlBase, keyspaceName, tableName),
            HttpStatus.SC_OK);
    ColumnDefinition[] columns = objectMapper.readValue(body, ColumnDefinition[].class);
    assertThat(columns)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .isEqualTo(new ColumnDefinition("id", "uuid", false)));
  }

  @Test
  public void columnsGetComplex() throws IOException {
    createTestKeyspace(keyspaceName);
    createComplexTestTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns",
                restUrlBase, keyspaceName, tableName),
            HttpStatus.SC_OK);
    ColumnDefinition[] columns = readWrappedRESTResponse(body, ColumnDefinition[].class);
    assertThat(columns)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .isEqualTo(new ColumnDefinition("col2", "frozen<set<boolean>>", false)));
  }

  @Test
  public void columnsGetBadTable() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns", restUrlBase, keyspaceName, "foo"),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnsGetBadKeyspace() throws IOException {
    createTestKeyspace(keyspaceName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns", restUrlBase, "foo", tableName),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnDelete() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    RestUtils.delete(
        authToken,
        String.format(
            "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
            restUrlBase, keyspaceName, tableName, "age"),
        HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void columnDeleteNotFound() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    RestUtils.delete(
        authToken,
        String.format(
            "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
            restUrlBase, keyspaceName, tableName, "foo"),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void columnDeleteBadTable() throws IOException {
    createTestKeyspace(keyspaceName);

    String body =
        RestUtils.delete(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, keyspaceName, "foo", "age"),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnDeleteBadKeyspace() throws IOException {
    String body =
        RestUtils.delete(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, "foo", tableName, "age"),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void columnDeletePartitionKey() throws IOException {
    createTestKeyspace(keyspaceName);
    createSimpleTestTable(keyspaceName, tableName);

    String body =
        RestUtils.delete(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                restUrlBase, keyspaceName, tableName, "id"),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  /*
  /************************************************************************
  /* Test methods for Index CRUD operations
  /************************************************************************
   */

  @Test
  public void indexCreateBasic(CqlSession session) throws IOException {
    createTestKeyspace(keyspaceName);
    tableName = "tbl_createtable_" + System.currentTimeMillis();
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text", "lastName text", "email list<text>"),
        Collections.singletonList("id"),
        null);

    IndexAdd indexAdd = new IndexAdd();
    indexAdd.setColumn("firstName");
    indexAdd.setName("test_idx");
    indexAdd.setIfNotExists(false);

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/indexes",
                restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_CREATED);
    SuccessResponse successResponse = objectMapper.readValue(body, SuccessResponse.class);
    assertThat(successResponse.getSuccess()).isTrue();

    List<Row> rows = session.execute("SELECT * FROM system_schema.indexes;").all();
    assertThat(rows.stream().anyMatch(i -> "test_idx".equals(i.getString("index_name")))).isTrue();

    // don't create an index if it already exists and don't throw error
    indexAdd.setIfNotExists(true);
    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/indexes",
                restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_CREATED);
    successResponse = objectMapper.readValue(body, SuccessResponse.class);
    assertThat(successResponse.getSuccess()).isTrue();

    // throw error if index already exists
    indexAdd.setIfNotExists(false);
    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/indexes",
                restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_BAD_REQUEST);

    ApiError response = objectMapper.readValue(body, ApiError.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    // 02-Feb-2022, tatu: Unfortunately exact error message from backend varies
    //     a bit across Cassandra versions (as well as SGv1/SGv2) so need to:
    assertThat(response.getDescription()).contains("test_idx").contains("already exists");

    // successfully index a collection
    indexAdd.setColumn("email");
    indexAdd.setName(null);
    indexAdd.setKind(IndexKind.VALUES);
    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/indexes",
                restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_CREATED);
    successResponse = objectMapper.readValue(body, SuccessResponse.class);
    assertThat(successResponse.getSuccess()).isTrue();
  }

  @Test
  public void indexCreateInvalid() throws IOException {
    createTestKeyspace(keyspaceName);
    String tableName = "tbl_createtable_" + System.currentTimeMillis();
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text", "email list<text>"),
        Collections.singletonList("id"),
        null);

    // invalid table
    IndexAdd indexAdd = new IndexAdd();
    indexAdd.setColumn("firstName");
    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/invalid_table/indexes",
                restUrlBase, keyspaceName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_BAD_REQUEST);
    ApiError response = objectMapper.readValue(body, ApiError.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).containsPattern("Table.*not found");

    // invalid column
    indexAdd.setColumn("invalid_column");
    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/indexes",
                restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_NOT_FOUND);

    response = objectMapper.readValue(body, ApiError.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(response.getDescription()).isEqualTo("Column 'invalid_column' not found in table.");

    // invalid index kind
    indexAdd.setColumn("firstName");
    indexAdd.setKind(IndexKind.ENTRIES);
    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/indexes",
                restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_BAD_REQUEST);

    response = objectMapper.readValue(body, ApiError.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).contains("Cannot create entries() index on firstName");
  }

  @Test
  public void indexListAll() throws IOException {
    createTestKeyspace(keyspaceName);
    tableName = "tbl_createtable_" + System.currentTimeMillis();
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text", "email list<text>"),
        Collections.singletonList("id"),
        null);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/indexes",
                restUrlBase, keyspaceName, tableName),
            HttpStatus.SC_OK);
    assertThat(body).isEqualTo("[]");

    IndexAdd indexAdd = new IndexAdd();
    indexAdd.setColumn("firstName");
    indexAdd.setName("test_idx");
    indexAdd.setIfNotExists(false);

    RestUtils.post(
        authToken,
        String.format(
            "%s/v2/schemas/keyspaces/%s/tables/%s/indexes", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(indexAdd),
        HttpStatus.SC_CREATED);

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/indexes",
                restUrlBase, keyspaceName, tableName),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});

    assertThat(data.stream().anyMatch(m -> "test_idx".equals(m.get("index_name")))).isTrue();
  }

  @Test
  public void indexDrop(CqlSession session) throws IOException {
    createTestKeyspace(keyspaceName);
    tableName = "tbl_createtable_" + System.currentTimeMillis();
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text", "email list<text>"),
        Collections.singletonList("id"),
        null);

    IndexAdd indexAdd = new IndexAdd();
    indexAdd.setColumn("firstName");
    indexAdd.setName("test_idx");
    indexAdd.setIfNotExists(false);

    RestUtils.post(
        authToken,
        String.format(
            "%s/v2/schemas/keyspaces/%s/tables/%s/indexes", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(indexAdd),
        HttpStatus.SC_CREATED);

    SimpleStatement selectIndexes =
        SimpleStatement.newInstance(
            "SELECT * FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?",
            keyspaceName,
            tableName);
    List<Row> rows = session.execute(selectIndexes).all();
    assertThat(rows.size()).isEqualTo(1);

    String indexName = "test_idx";
    RestUtils.delete(
        authToken,
        String.format(
            "%s/v2/schemas/keyspaces/%s/tables/%s/indexes/%s",
            restUrlBase, keyspaceName, tableName, indexName),
        HttpStatus.SC_NO_CONTENT);

    rows = session.execute(selectIndexes).all();
    assertThat(rows.size()).isEqualTo(0);

    indexName = "invalid_idx";
    String body =
        RestUtils.delete(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/indexes/%s",
                restUrlBase, keyspaceName, tableName, indexName),
            HttpStatus.SC_NOT_FOUND);

    ApiError response = objectMapper.readValue(body, ApiError.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(response.getDescription()).isEqualTo("Index 'invalid_idx' not found.");

    // ifExists=true
    indexName = "invalid_idx";
    RestUtils.delete(
        authToken,
        String.format(
            "%s/v2/schemas/keyspaces/%s/tables/%s/indexes/%s?ifExists=true",
            restUrlBase, keyspaceName, tableName, indexName),
        HttpStatus.SC_NO_CONTENT);
  }

  /*
  /************************************************************************
  /* Test methods for User-Defined Type (UDT) CRUD operations
  /************************************************************************
   */

  @Test
  public void udtCreateBasic() throws IOException {
    createTestKeyspace(keyspaceName);

    // create UDT
    String udtString =
        "{\"name\": \"udt1\", \"fields\":"
            + "[{\"name\":\"firstname\",\"typeDefinition\":\"text\"},"
            + "{\"name\":\"birthdate\",\"typeDefinition\":\"date\"}]}";

    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_CREATED);

    // throws error because same name, but ifNotExists = false
    String response =
        RestUtils.post(
            authToken,
            String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
            udtString,
            HttpStatus.SC_BAD_REQUEST);

    String typeName = "udt1";

    ApiError apiError = objectMapper.readValue(response, ApiError.class);
    assertThat(apiError.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    // Sample output:
    //  Cassandra 4.0: “Bad request: A user type with name ‘udt1’ already exists”
    //  Cassandra 3.11, DSE 6.8: “Bad request: A user type of name
    //     ks_udtCreateBasic_1643916413499.udt1 already exists”
    assertThat(apiError.getDescription())
        .matches(String.format("Bad request: A user type .*%s.* already exists", typeName));

    // don't create and don't throw exception because ifNotExists = true
    udtString =
        "{\"name\": \"udt1\", \"ifNotExists\": true,"
            + "\"fields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";
    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_CREATED);
  }

  @Test
  public void udtCreateInvalid() throws IOException {
    createTestKeyspace(keyspaceName);

    String udtString =
        "{\"name\": \"udt1\", \"fields\":[{\"name\":\"firstname\",\"typeDefinition\":\"invalid_type\"}}]}";
    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_BAD_REQUEST);

    udtString = "{\"name\": \"udt1\", \"fields\":[]}";
    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_BAD_REQUEST);

    udtString = "{\"name\": \"udt1\", \"fields\":[{\"name\":\"firstname\"}}]}";
    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_BAD_REQUEST);

    udtString = "{\"name\": \"udt1\", \"fields\":[{\"typeDefinition\":\"text\"}}]}";
    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void udtUpdateBasic() throws IOException {
    createTestKeyspace(keyspaceName);

    // create UDT
    String udtString =
        "{\"name\": \"udt1\", \"fields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";

    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_CREATED);

    // update UDT: add new field
    udtString =
        "{\"name\": \"udt1\", \"addFields\":[{\"name\":\"lastname\",\"typeDefinition\":\"text\"}]}";

    RestUtils.put(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_OK);

    // udpate UDT: rename fields
    udtString =
        "{\"name\": \"udt1\",\"renameFields\":"
            + "[{\"from\":\"firstname\",\"to\":\"name1\"}, {\"from\":\"lastname\",\"to\":\"name2\"}]}";

    RestUtils.put(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_OK);

    // retrieve UDT
    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces/%s/types/%s", restUrlBase, keyspaceName, "udt1"),
            HttpStatus.SC_OK);

    MapGetResponseWrapper getResponseWrapper =
        objectMapper.readValue(body, MapGetResponseWrapper.class);
    Map<String, Object> response = getResponseWrapper.getData();

    assertThat(response.size()).isEqualTo(3);
    assertThat(response.get("name")).isEqualTo("udt1");
    List<Map<String, String>> fields = (List<Map<String, String>>) response.get("fields");
    assertThat(fields.size()).isEqualTo(2);

    Set<String> fieldNames = new HashSet<>();
    fieldNames.add(fields.get(0).get("name"));
    fieldNames.add(fields.get(1).get("name"));

    assertThat(fieldNames.contains("name1")).isTrue();
    assertThat(fieldNames.contains("name2")).isTrue();

    // create UDT
    udtString = "{\"name\": \"udt2\", \"fields\":[{\"name\":\"age\",\"typeDefinition\":\"int\"}]}";

    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_CREATED);

    // update UDT: add and rename field
    udtString =
        "{\"name\": \"udt2\","
            + "\"addFields\":[{\"name\":\"name\",\"typeDefinition\":\"text\"}]},"
            + "\"renameFields\": [{\"from\": \"name\", \"to\": \"firstname\"}";

    RestUtils.put(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_OK);
  }

  @Test
  public void udtUpdateInvalid() throws IOException {
    createTestKeyspace(keyspaceName);

    // create UDT
    String udtString =
        "{\"name\": \"udt1\", \"fields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";

    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_CREATED);

    // add existing field
    udtString =
        "{\"name\": \"udt1\", \"addFields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";

    RestUtils.put(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_BAD_REQUEST);

    // missing add-type and rename-type
    udtString =
        "{\"name\": \"udt1\", \"fields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";

    RestUtils.put(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void udtGetOne() throws IOException {
    createTestKeyspace(keyspaceName);

    String udtString =
        "{\"name\": \"test_udt1\", \"fields\":"
            + "[{\"name\":\"arrival\",\"typeDefinition\":\"timestamp\"},"
            + "{\"name\":\"props\",\"typeDefinition\":\"frozen<map<text,text>>\"}]}";

    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_CREATED);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/types/%s", restUrlBase, keyspaceName, "test_udt1"),
            HttpStatus.SC_OK);

    MapGetResponseWrapper getResponseWrapper =
        objectMapper.readValue(body, MapGetResponseWrapper.class);
    Map<String, Object> response = getResponseWrapper.getData();

    assertThat(response.size()).isEqualTo(3);
    assertThat(response.get("name")).isEqualTo("test_udt1");
    List<Map<String, String>> fields = (List<Map<String, String>>) response.get("fields");
    assertThat(fields.size()).isEqualTo(2);
    assertThat(fields.get(0).get("name")).isEqualTo("arrival");
    assertThat(fields.get(0).get("typeDefinition")).isEqualTo("timestamp");
    assertThat(fields.get(1).get("name")).isEqualTo("props");
    assertThat(fields.get(1).get("typeDefinition")).isEqualTo("frozen<map<text, text>>");

    // Also try to access non-existing one to verify correct HTTP status code (404)
    RestUtils.get(
        authToken,
        String.format(
            "%s/v2/schemas/keyspaces/%s/types/%s", restUrlBase, keyspaceName, "invalid_udt"),
        HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void udtGetAll() throws IOException {
    createTestKeyspace(keyspaceName);

    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
            HttpStatus.SC_OK);

    ListOfMapsGetResponseWrapper getResponseWrapper =
        objectMapper.readValue(body, ListOfMapsGetResponseWrapper.class);
    List<Map<String, Object>> response = getResponseWrapper.getData();
    assertThat(response.size()).isEqualTo(0);

    // creates 10 UDTs
    String udtString =
        "{\"name\": \"%s\", \"fields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";
    for (int i = 0; i < 10; i++) {
      RestUtils.post(
          authToken,
          String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
          String.format(udtString, "udt" + i),
          HttpStatus.SC_CREATED);
    }

    body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
            HttpStatus.SC_OK);

    getResponseWrapper = objectMapper.readValue(body, ListOfMapsGetResponseWrapper.class);
    response = getResponseWrapper.getData();
    assertThat(response.size()).isEqualTo(10);

    List<Map<String, String>> fields = (List<Map<String, String>>) response.get(0).get("fields");
    assertThat(fields.size()).isEqualTo(1);
    assertThat(fields.get(0).get("name")).isEqualTo("firstname");
    assertThat(fields.get(0).get("typeDefinition")).isEqualTo("text");
  }

  @Test
  public void udtDelete() throws IOException {
    createTestKeyspace(keyspaceName);

    String udtString =
        "{\"name\": \"test_udt1\", \"fields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";

    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_CREATED);

    RestUtils.delete(
        authToken,
        String.format(
            "%s/v2/schemas/keyspaces/%s/types/%s", restUrlBase, keyspaceName, "test_udt1"),
        HttpStatus.SC_NO_CONTENT);

    // delete a non existent UDT
    RestUtils.delete(
        authToken,
        String.format(
            "%s/v2/schemas/keyspaces/%s/types/%s", restUrlBase, keyspaceName, "test_udt1"),
        HttpStatus.SC_BAD_REQUEST);

    // delete an UDT in use
    udtString =
        "{\"name\": \"fullname\", \"fields\":"
            + "[{\"name\":\"firstname\",\"typeDefinition\":\"text\"},"
            + "{\"name\":\"lastname\",\"typeDefinition\":\"text\"}]}";
    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_CREATED);

    createTestTable(
        tableName,
        Arrays.asList("id text", "name fullname"),
        Collections.singletonList("id"),
        null);

    String res =
        RestUtils.delete(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/types/%s", restUrlBase, keyspaceName, "fullname"),
            HttpStatus.SC_BAD_REQUEST);
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

  private void createTestTable(
      String tableName, List<String> columns, List<String> partitionKey, List<String> clusteringKey)
      throws IOException {
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions =
        columns.stream()
            .map(x -> x.split(" "))
            .map(y -> new ColumnDefinition(y[0], y[1]))
            .collect(Collectors.toList());
    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(partitionKey);
    if (clusteringKey != null) {
      primaryKey.setClusteringKey(clusteringKey);
    }
    tableAdd.setPrimaryKey(primaryKey);

    String body =
        RestUtils.post(
            authToken,
            String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
            objectMapper.writeValueAsString(tableAdd),
            HttpStatus.SC_CREATED);

    TableResponse tableResponse = objectMapper.readValue(body, TableResponse.class);
    assertThat(tableResponse.getName()).isEqualTo(tableName);
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

  private void createTestTableWithClustering(String keyspaceName, String tableName)
      throws IOException {
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("id", "int"));
    columnDefinitions.add(new ColumnDefinition("lastName", "text"));
    columnDefinitions.add(new ColumnDefinition("firstName", "text"));
    columnDefinitions.add(new ColumnDefinition("age", "int", true));
    columnDefinitions.add(new ColumnDefinition("expense_id", "int"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("id"));
    primaryKey.setClusteringKey(Collections.singletonList("expense_id"));
    tableAdd.setPrimaryKey(primaryKey);

    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_CREATED);
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
