/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.web.models.ColumnDefinition;
import io.stargate.web.models.Error;
import io.stargate.web.models.GetResponseWrapper;
import io.stargate.web.models.IndexAdd;
import io.stargate.web.models.IndexKind;
import io.stargate.web.models.Keyspace;
import io.stargate.web.models.PrimaryKey;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.models.SuccessResponse;
import io.stargate.web.models.TableAdd;
import io.stargate.web.models.TableOptions;
import io.stargate.web.models.TableResponse;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@NotThreadSafe
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec()
public class RestApiv2Test extends BaseOsgiIntegrationTest {

  private String keyspaceName;
  private String tableName;
  private static String authToken;
  private String host;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  public void setup(TestInfo testInfo, StargateConnectionInfo cluster) throws IOException {
    host = "http://" + cluster.seedAddress();

    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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

  @Test
  public void getKeyspaces() throws IOException {
    String body =
        RestUtils.get(
            authToken, String.format("%s:8082/v2/schemas/keyspaces", host), HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    List<Keyspace> keyspaces =
        objectMapper.convertValue(response.getData(), new TypeReference<List<Keyspace>>() {});
    assertThat(keyspaces)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .isEqualTo(new Keyspace("system", null)));
  }

  @Test
  public void getKeyspacesMissingToken() throws IOException {
    RestUtils.get(
        "", String.format("%s:8082/v2/schemas/keyspaces", host), HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void getKeyspacesBadToken() throws IOException {
    RestUtils.get(
        "foo", String.format("%s:8082/v2/schemas/keyspaces", host), HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void getKeyspacesRaw() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/keyspaces?raw=true", host),
            HttpStatus.SC_OK);

    List<Keyspace> keyspaces = objectMapper.readValue(body, new TypeReference<List<Keyspace>>() {});
    assertThat(keyspaces)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .isEqualTo(new Keyspace("system_schema", null)));
  }

  @Test
  public void getKeyspace() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/keyspaces/system", host),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    Keyspace keyspace = objectMapper.convertValue(response.getData(), Keyspace.class);

    assertThat(keyspace).usingRecursiveComparison().isEqualTo(new Keyspace("system", null));
  }

  @Test
  public void getKeyspaceRaw() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/keyspaces/system?raw=true", host),
            HttpStatus.SC_OK);

    Keyspace keyspace = objectMapper.readValue(body, Keyspace.class);

    assertThat(keyspace).usingRecursiveComparison().isEqualTo(new Keyspace("system", null));
  }

  @Test
  public void getKeyspaceNotFound() throws IOException {
    RestUtils.get(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/ks_not_found", host),
        HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void createKeyspace() throws IOException {
    String keyspaceName = "ks_createkeyspace_" + System.currentTimeMillis();
    createKeyspace(keyspaceName);

    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/keyspaces/%s?raw=true", host, keyspaceName),
            HttpStatus.SC_OK);

    Keyspace keyspace = objectMapper.readValue(body, Keyspace.class);

    assertThat(keyspace).usingRecursiveComparison().isEqualTo(new Keyspace(keyspaceName, null));
  }

  @Test
  public void deleteKeyspace() throws IOException {
    String keyspaceName = "ks_createkeyspace_" + System.currentTimeMillis();
    createKeyspace(keyspaceName);

    RestUtils.get(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s", host, keyspaceName),
        HttpStatus.SC_OK);

    RestUtils.delete(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s", host, keyspaceName),
        HttpStatus.SC_NO_CONTENT);

    RestUtils.get(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s", host, keyspaceName),
        HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void getTables() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/keyspaces/system/tables", host),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    List<TableResponse> tables =
        objectMapper.convertValue(response.getData(), new TypeReference<List<TableResponse>>() {});

    assertThat(tables.size()).isGreaterThan(5);
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
  public void getTablesRaw() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/keyspaces/system/tables?raw=true", host),
            HttpStatus.SC_OK);

    List<TableResponse> tables =
        objectMapper.readValue(body, new TypeReference<List<TableResponse>>() {});

    assertThat(tables.size()).isGreaterThan(5);
    assertThat(tables)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .ignoringFields("columnDefinitions", "primaryKey", "tableOptions")
                    .isEqualTo(new TableResponse("local", "system", null, null, null)));
  }

  @Test
  public void getTable() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/keyspaces/system/tables/local", host),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    TableResponse table = objectMapper.convertValue(response.getData(), TableResponse.class);
    assertThat(table.getKeyspace()).isEqualTo("system");
    assertThat(table.getName()).isEqualTo("local");
    assertThat(table.getColumnDefinitions()).isNotNull();
  }

  @Test
  public void getTableRaw() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/schemas/keyspaces/system/tables/local?raw=true", host),
            HttpStatus.SC_OK);

    TableResponse table = objectMapper.readValue(body, TableResponse.class);
    assertThat(table.getKeyspace()).isEqualTo("system");
    assertThat(table.getName()).isEqualTo("local");
    assertThat(table.getColumnDefinitions()).isNotNull();
  }

  @Test
  public void getTableComplex() throws IOException {
    createKeyspace(keyspaceName);
    createComplexTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    TableResponse table = objectMapper.convertValue(response.getData(), TableResponse.class);
    assertThat(table.getKeyspace()).isEqualTo(keyspaceName);
    assertThat(table.getName()).isEqualTo(tableName);
    assertThat(table.getColumnDefinitions()).isNotNull();
    ColumnDefinition columnDefinition =
        table.getColumnDefinitions().stream()
            .filter(c -> c.getName().equals("col1"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Column not found"));
    assertThat(columnDefinition)
        .usingRecursiveComparison()
        .isEqualTo(new ColumnDefinition("col1", "frozen<map<date, varchar>>", false));
  }

  @Test
  public void getTableNotFound() throws IOException {
    RestUtils.get(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/system/tables/tbl_not_found", host),
        HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void createTable() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s?raw=true",
                host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    TableResponse table = objectMapper.readValue(body, TableResponse.class);
    assertThat(table.getKeyspace()).isEqualTo(keyspaceName);
    assertThat(table.getName()).isEqualTo(tableName);
    assertThat(table.getColumnDefinitions()).isNotNull();
  }

  @Test
  public void updateTable() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    TableAdd tableUpdate = new TableAdd();
    tableUpdate.setName(tableName);

    TableOptions tableOptions = new TableOptions();
    tableOptions.setDefaultTimeToLive(5);
    tableUpdate.setTableOptions(tableOptions);

    RestUtils.put(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s/tables/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(tableUpdate),
        HttpStatus.SC_CREATED);
  }

  @Test
  public void deleteTable() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    RestUtils.delete(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s/tables/%s", host, keyspaceName, tableName),
        HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void createIndex(CqlSession session) throws IOException {
    createKeyspace(keyspaceName);
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
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_CREATED);
    SuccessResponse successResponse =
        objectMapper.readValue(body, new TypeReference<SuccessResponse>() {});
    assertThat(successResponse.getSuccess()).isTrue();

    List<Row> rows = session.execute("SELECT * FROM system_schema.indexes;").all();
    assertThat(rows.stream().anyMatch(i -> "test_idx".equals(i.getString("index_name")))).isTrue();

    // don't create and index if it already exists and don't throw error
    indexAdd.setIfNotExists(true);
    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_CREATED);
    successResponse = objectMapper.readValue(body, new TypeReference<SuccessResponse>() {});
    assertThat(successResponse.getSuccess()).isTrue();

    // throw error if index already exists
    indexAdd.setIfNotExists(false);
    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_BAD_REQUEST);

    Error response = objectMapper.readValue(body, Error.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription())
        .isEqualTo("Bad request: An index named test_idx already exists");

    // successufully index a collection
    indexAdd.setColumn("email");
    indexAdd.setName(null);
    indexAdd.setKind(IndexKind.VALUES);
    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_CREATED);
    successResponse = objectMapper.readValue(body, new TypeReference<SuccessResponse>() {});
    assertThat(successResponse.getSuccess()).isTrue();
  }

  @Test
  public void createCustomIndex(CqlSession session) throws IOException {
    // TODO remove this when we figure out how to enable SAI indexes in Cassandra 4
    assumeThat(isCassandra4())
        .as(
            "Disabled because it is currently not possible to enable SAI indexes "
                + "on a Cassandra 4 backend")
        .isFalse();

    createKeyspace(keyspaceName);
    tableName = "tbl_createtable_" + System.currentTimeMillis();
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text", "lastName text", "email list<text>"),
        Collections.singletonList("id"),
        null);

    IndexAdd indexAdd = new IndexAdd();
    String indexType = "org.apache.cassandra.index.sasi.SASIIndex";
    indexAdd.setColumn("lastName");
    indexAdd.setName("test_custom_idx");
    indexAdd.setType(indexType);
    indexAdd.setIfNotExists(false);
    indexAdd.setKind(null);

    Map<String, String> options = new HashMap<>();
    options.put("mode", "CONTAINS");
    indexAdd.setOptions(options);

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_CREATED);
    SuccessResponse successResponse =
        objectMapper.readValue(body, new TypeReference<SuccessResponse>() {});
    assertThat(successResponse.getSuccess()).isTrue();

    Collection<Row> rows = session.execute("SELECT * FROM system_schema.indexes;").all();
    Optional<Row> row =
        rows.stream().filter(i -> "test_custom_idx".equals(i.getString("index_name"))).findFirst();
    Map<String, String> optionsReturned = row.get().getMap("options", String.class, String.class);

    assertThat(optionsReturned.get("class_name")).isEqualTo(indexType);
    assertThat(optionsReturned.get("target")).isEqualTo("\"lastName\"");
    assertThat(optionsReturned.get("mode")).isEqualTo("CONTAINS");
  }

  @Test
  public void createInvalidIndex() throws IOException {
    createKeyspace(keyspaceName);
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
                "%s:8082/v2/schemas/keyspaces/%s/tables/invalid_table/indexes", host, keyspaceName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_NOT_FOUND);
    Error response = objectMapper.readValue(body, Error.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(response.getDescription()).isEqualTo("Table 'invalid_table' not found in keyspace.");

    // invalid column
    indexAdd.setColumn("invalid_column");
    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_NOT_FOUND);

    response = objectMapper.readValue(body, Error.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(response.getDescription()).isEqualTo("Column 'invalid_column' not found in table.");

    // invalid index kind
    indexAdd.setColumn("firstName");
    indexAdd.setKind(IndexKind.ENTRIES);
    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_BAD_REQUEST);

    response = objectMapper.readValue(body, Error.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription())
        .isEqualTo("Bad request: Indexing entries can only be used with a map");
  }

  @Test
  public void listAllIndexes(CqlSession session) throws IOException {
    createKeyspace(keyspaceName);
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
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
            HttpStatus.SC_OK);
    assertThat(body).isEqualTo("[]");

    IndexAdd indexAdd = new IndexAdd();
    indexAdd.setColumn("firstName");
    indexAdd.setName("test_idx");
    indexAdd.setIfNotExists(false);

    RestUtils.post(
        authToken,
        String.format(
            "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(indexAdd),
        HttpStatus.SC_CREATED);

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});

    assertThat(data.stream().anyMatch(m -> "test_idx".equals(m.get("index_name")))).isTrue();
  }

  @Test
  public void dropIndex(CqlSession session) throws IOException {
    createKeyspace(keyspaceName);
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
            "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes", host, keyspaceName, tableName),
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
            "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes/%s",
            host, keyspaceName, tableName, indexName),
        HttpStatus.SC_NO_CONTENT);

    rows = session.execute(selectIndexes).all();
    assertThat(rows.size()).isEqualTo(0);

    indexName = "invalid_idx";
    String body =
        RestUtils.delete(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes/%s",
                host, keyspaceName, tableName, indexName),
            HttpStatus.SC_NOT_FOUND);

    Error response = objectMapper.readValue(body, Error.class);
    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(response.getDescription()).isEqualTo("Index 'invalid_idx' not found.");

    // ifExists=true
    indexName = "invalid_idx";
    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v2/schemas/keyspaces/%s/tables/%s/indexes/%s?ifExists=true",
            host, keyspaceName, tableName, indexName),
        HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void createTableWithNullOptions() throws IOException {
    createKeyspace(keyspaceName);

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
            String.format("%s:8082/v2/schemas/keyspaces/%s/tables", host, keyspaceName),
            objectMapper.writeValueAsString(tableAdd),
            HttpStatus.SC_CREATED);

    SuccessResponse successResponse =
        objectMapper.readValue(body, new TypeReference<SuccessResponse>() {});
    assertThat(successResponse.getSuccess()).isTrue();
  }

  @Test
  public void getRowsWithQuery() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s?where=%s", host, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
  }

  @Test
  public void getRowsWithQueryAndPaging() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s?where=%s&page-size=1",
                host, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(getResponseWrapper.getPageState()).isNotEmpty();
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
    assertThat(data.get(0).get("expense_id")).isEqualTo(1);
  }

  @Test
  public void getRowsWithQueryAndRaw() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s?where=%s&raw=true",
                host, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
  }

  @Test
  public void getRowsWithQueryAndSort() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s?where=%s&sort={\"expense_id\":\"desc\"}",
                host, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
    assertThat(data.get(0).get("expense_id")).isEqualTo(2);
  }

  @Test
  public void getRowsWithQueryRawAndSort() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s?where=%s&sort={\"expense_id\":\"desc\"}&raw=true",
                host, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.size()).isEqualTo(2);
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
    assertThat(data.get(0).get("expense_id")).isEqualTo(2);
  }

  @Test
  public void getRowsWithNotFound() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    String whereClause = "{\"id\":{\"$eq\":\"f0014be3-b69f-4884-b9a6-49765fb40df3\"}}";
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s?where=%s", host, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(0);
    assertThat(data).isEmpty();
  }

  @Test
  public void getInvalidWhereClause() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    String whereClause = "{\"invalid_field\":{\"$eq\":\"test\"}}";
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s?where=%s", host, keyspaceName, tableName, whereClause),
            HttpStatus.SC_BAD_REQUEST);

    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription())
        .isEqualTo("Bad request: Unknown field name 'invalid_field' in where clause.");
  }

  @Test
  public void getRows() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
  }

  @Test
  public void getRowsSort() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?sort={\"expense_id\":\"desc\"}",
                host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
    assertThat(data.get(0).get("expense_id")).isEqualTo(2);
  }

  @Test
  public void getRowsPaging() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?page-size=1",
                host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(getResponseWrapper.getPageState()).isNotEmpty();
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
    assertThat(data.get(0).get("expense_id")).isEqualTo(1);
  }

  @Test
  public void getRowsNotFound() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s",
                host, keyspaceName, tableName, "f0014be3-b69f-4884-b9a6-49765fb40df3"),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(0);
    assertThat(data).isEmpty();
  }

  @Test
  public void getRowsRaw() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?raw=true",
                host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
  }

  @Test
  public void getRowsRawAndSort() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?sort={\"expense_id\": \"desc\"}&raw=true",
                host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.size()).isEqualTo(2);
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
    assertThat(data.get(0).get("expense_id")).isEqualTo(2);
  }

  @Test
  public void getRowsPartitionKeyOnly() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("expense_id")).isEqualTo(1);
    assertThat(data.get(1).get("id")).isEqualTo(1);
    assertThat(data.get(1).get("expense_id")).isEqualTo(2);
  }

  @Test
  public void getRowsPartitionAndClusterKeys() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s/2", host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
    assertThat(data.get(0).get("expense_id")).isEqualTo(2);
  }

  @Test
  public void getRowsWithMixedClustering() throws IOException {
    setupMixedClusteringTestCase();

    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    assertThat(data.get(0).get("v")).isEqualTo(9);
    assertThat(data.get(1).get("v")).isEqualTo(19);

    body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1/20", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(data.get(0).get("v")).isEqualTo(19);
  }

  @Test
  public void addRow() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    String body =
        RestUtils.post(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(row),
            HttpStatus.SC_CREATED);

    Map<String, Object> rowResponse =
        objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {});
    assertThat(rowResponse.get("id")).isEqualTo(rowIdentifier);
  }

  @Test
  public void addRowWithCounter() throws IOException {
    createKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "counter counter"),
        Collections.singletonList("id"),
        null);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("counter", "+1");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void addRowWithList() throws IOException {
    createKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("name text", "email list<text>"),
        Collections.singletonList("name"),
        null);

    Map<String, String> row = new HashMap<>();
    row.put("name", "alice");
    row.put("email", "['foo@example.com','bar@example.com']");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?raw=true", host, keyspaceName, tableName, "alice"),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.get(0).get("name")).isEqualTo("alice");
    assertThat(data.get(0).get("email"))
        .isEqualTo(Arrays.asList("foo@example.com", "bar@example.com"));
  }

  @Test
  public void addRowInvalidField() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("invalid_field", "John");

    String body =
        RestUtils.post(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(row),
            HttpStatus.SC_BAD_REQUEST);

    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription())
        .isEqualTo("Bad request: Unknown field name 'invalid_field'.");
  }

  @Test
  public void updateRow() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("firstName", "Robert");
    rowUpdate.put("lastName", "Plant");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            objectMapper.writeValueAsString(rowUpdate),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    ResponseWrapper responseWrapper = objectMapper.readValue(body, ResponseWrapper.class);
    @SuppressWarnings("unchecked")
    Map<String, String> data = objectMapper.convertValue(responseWrapper.getData(), Map.class);

    assertThat(data).containsAllEntriesOf(rowUpdate);
  }

  @Test
  public void updateRowRaw() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("firstName", "Robert");
    rowUpdate.put("lastName", "Plant");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?raw=true",
                host, keyspaceName, tableName, rowIdentifier),
            objectMapper.writeValueAsString(rowUpdate),
            HttpStatus.SC_OK);

    @SuppressWarnings("unchecked")
    Map<String, String> data = objectMapper.readValue(body, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);
  }

  @Test
  public void updateRowWithCounter() throws IOException {
    createKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "counter counter"),
        Collections.singletonList("id"),
        null);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = Collections.singletonMap("counter", "+1");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?raw=true",
                host, keyspaceName, tableName, rowIdentifier),
            objectMapper.writeValueAsString(row),
            HttpStatus.SC_OK);

    @SuppressWarnings("unchecked")
    Map<String, String> data = objectMapper.readValue(body, Map.class);
    assertThat(data).containsAllEntriesOf(row);

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?raw=true",
                host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    List<Map<String, Object>> dataList =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(dataList.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(dataList.get(0).get("counter")).isEqualTo("1");

    body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?raw=true",
                host, keyspaceName, tableName, rowIdentifier),
            objectMapper.writeValueAsString(row),
            HttpStatus.SC_OK);

    @SuppressWarnings("unchecked")
    Map<String, String> dataMap = objectMapper.readValue(body, Map.class);
    assertThat(dataMap).containsAllEntriesOf(row);

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?raw=true",
                host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    dataList = objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(dataList.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(dataList.get(0).get("counter")).isEqualTo("2");
  }

  @Test
  public void updateRowWithMultipleCounters() throws IOException {
    createKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "counter1 counter", "counter2 counter"),
        Collections.singletonList("id"),
        null);

    String rowIdentifier = UUID.randomUUID().toString();

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("counter1", "+1");
    rowUpdate.put("counter2", "-1");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?raw=true",
                host, keyspaceName, tableName, rowIdentifier),
            objectMapper.writeValueAsString(rowUpdate),
            HttpStatus.SC_OK);

    @SuppressWarnings("unchecked")
    Map<String, String> data = objectMapper.readValue(body, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?raw=true",
                host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    List<Map<String, Object>> dataList =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(dataList.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(dataList.get(0).get("counter1")).isEqualTo("1");
    assertThat(dataList.get(0).get("counter2")).isEqualTo("-1");
  }

  @Test
  public void patchRow() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");
    row.put("lastName", "Doe");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("firstName", "Jane");

    String body =
        RestUtils.patch(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            objectMapper.writeValueAsString(rowUpdate),
            HttpStatus.SC_OK);
    @SuppressWarnings("rawtypes")
    ResponseWrapper responseWrapper = objectMapper.readValue(body, ResponseWrapper.class);
    @SuppressWarnings("unchecked")
    Map<String, String> patchData = objectMapper.convertValue(responseWrapper.getData(), Map.class);

    assertThat(patchData).containsAllEntriesOf(rowUpdate);

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("firstName")).isEqualTo("Jane");
    assertThat(data.get(0).get("lastName")).isEqualTo("Doe");
  }

  @Test
  public void patchRowRaw() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");
    row.put("lastName", "Doe");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("firstName", "Jane");

    String body =
        RestUtils.patch(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s?raw=true",
                host, keyspaceName, tableName, rowIdentifier),
            objectMapper.writeValueAsString(rowUpdate),
            HttpStatus.SC_OK);
    @SuppressWarnings("unchecked")
    Map<String, String> patchData = objectMapper.readValue(body, Map.class);

    assertThat(patchData).containsAllEntriesOf(rowUpdate);

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("firstName")).isEqualTo("Jane");
    assertThat(data.get(0).get("lastName")).isEqualTo("Doe");
  }

  @Test
  public void deleteRow() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
        HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void deleteRowClustering() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("expense_id")).isEqualTo(1);
    assertThat(data.get(1).get("id")).isEqualTo(1);
    assertThat(data.get(1).get("expense_id")).isEqualTo(2);

    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s/%s/1", host, keyspaceName, tableName, rowIdentifier),
        HttpStatus.SC_NO_CONTENT);

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("expense_id")).isEqualTo(2);
  }

  @Test
  public void deleteRowByPartitionKey() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("expense_id")).isEqualTo(1);
    assertThat(data.get(1).get("id")).isEqualTo(1);
    assertThat(data.get(1).get("expense_id")).isEqualTo(2);

    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
        HttpStatus.SC_NO_CONTENT);

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    assertThat(getResponseWrapper.getCount()).isEqualTo(0);

    body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, "2"),
            HttpStatus.SC_OK);

    objectMapper.readValue(body, GetResponseWrapper.class);
    getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(data.get(0).get("id")).isEqualTo(2);
    assertThat(data.get(0).get("firstName")).isEqualTo("Jane");
  }

  @Test
  public void deleteRowsWithMixedClustering() throws IOException {
    setupMixedClusteringTestCase();

    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    assertThat(data.get(0).get("v")).isEqualTo(9);
    assertThat(data.get(1).get("v")).isEqualTo(19);

    RestUtils.delete(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1", host, keyspaceName, tableName),
        HttpStatus.SC_NO_CONTENT);

    body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    assertThat(getResponseWrapper.getCount()).isEqualTo(0);
  }

  @Test
  public void deleteRowsMixedClusteringAndCK() throws IOException {
    setupMixedClusteringTestCase();

    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    assertThat(data.get(0).get("v")).isEqualTo(9);
    assertThat(data.get(1).get("v")).isEqualTo(19);

    RestUtils.delete(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1/20", host, keyspaceName, tableName),
        HttpStatus.SC_NO_CONTENT);

    body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1/20", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    assertThat(getResponseWrapper.getCount()).isEqualTo(0);

    body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(data.get(0).get("v")).isEqualTo(9);
  }

  @Test
  public void getColumns() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, tableName),
            HttpStatus.SC_OK);
    @SuppressWarnings("rawtypes")
    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    List<ColumnDefinition> columns =
        objectMapper.convertValue(
            response.getData(), new TypeReference<List<ColumnDefinition>>() {});
    assertThat(columns)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .isEqualTo(new ColumnDefinition("id", "uuid", false)));
  }

  @Test
  public void getColumnsRaw() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns?raw=true",
                host, keyspaceName, tableName),
            HttpStatus.SC_OK);
    List<ColumnDefinition> columns =
        objectMapper.readValue(body, new TypeReference<List<ColumnDefinition>>() {});
    assertThat(columns)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .isEqualTo(new ColumnDefinition("id", "uuid", false)));
  }

  @Test
  public void getColumnsComplex() throws IOException {
    createKeyspace(keyspaceName);
    createComplexTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, tableName),
            HttpStatus.SC_OK);
    @SuppressWarnings("rawtypes")
    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    List<ColumnDefinition> columns =
        objectMapper.convertValue(
            response.getData(), new TypeReference<List<ColumnDefinition>>() {});
    assertThat(columns)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .isEqualTo(new ColumnDefinition("col2", "frozen<set<boolean>>", false)));
  }

  @Test
  public void getColumnsBadTable() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, "foo"),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void getColumnsBadKeyspace() throws IOException {
    createKeyspace(keyspaceName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns", host, "foo", tableName),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void getColumn() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, keyspaceName, tableName, "age"),
            HttpStatus.SC_OK);
    @SuppressWarnings("rawtypes")
    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    ColumnDefinition column = objectMapper.convertValue(response.getData(), ColumnDefinition.class);
    assertThat(column)
        .usingRecursiveComparison()
        .isEqualTo(new ColumnDefinition("age", "int", false));
  }

  @Test
  public void getColumnNotFound() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, keyspaceName, tableName, "foo"),
            HttpStatus.SC_NOT_FOUND);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void getColumnRaw() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true",
                host, keyspaceName, tableName, "age"),
            HttpStatus.SC_OK);
    ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
    assertThat(column)
        .usingRecursiveComparison()
        .isEqualTo(new ColumnDefinition("age", "int", false));
  }

  @Test
  public void getColumnComplex() throws IOException {
    createKeyspace(keyspaceName);
    createComplexTable(keyspaceName, tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, keyspaceName, tableName, "col1"),
            HttpStatus.SC_OK);
    @SuppressWarnings("rawtypes")
    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    ColumnDefinition column = objectMapper.convertValue(response.getData(), ColumnDefinition.class);
    assertThat(column)
        .usingRecursiveComparison()
        .isEqualTo(new ColumnDefinition("col1", "frozen<map<date, varchar>>", false));
  }

  @Test
  public void getColumnBadTable() throws IOException {
    createKeyspace(keyspaceName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, keyspaceName, "foo", "age"),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void getColumnBadKeyspace() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, "foo", tableName, "age"),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void addColumn() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    ColumnDefinition columnDefinition = new ColumnDefinition("name", "varchar");

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_CREATED);
    @SuppressWarnings("unchecked")
    Map<String, String> response = objectMapper.readValue(body, Map.class);

    assertThat(response.get("name")).isEqualTo("name");

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true",
                host, keyspaceName, tableName, "name"),
            HttpStatus.SC_OK);
    ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
    assertThat(column).usingRecursiveComparison().isEqualTo(columnDefinition);
  }

  @Test
  public void addColumnBadType() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    ColumnDefinition columnDefinition = new ColumnDefinition("name", "badType");

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void addColumnStatic() throws IOException {
    createKeyspace(keyspaceName);
    createTableWithClustering(keyspaceName, tableName);

    ColumnDefinition columnDefinition = new ColumnDefinition("balance", "float", true);

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_CREATED);
    @SuppressWarnings("unchecked")
    Map<String, String> response = objectMapper.readValue(body, Map.class);

    assertThat(response.get("name")).isEqualTo("balance");

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true",
                host, keyspaceName, tableName, "balance"),
            HttpStatus.SC_OK);
    ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
    assertThat(column).usingRecursiveComparison().isEqualTo(columnDefinition);
  }

  @Test
  public void updateColumn() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    ColumnDefinition columnDefinition = new ColumnDefinition("identifier", "uuid");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, keyspaceName, tableName, "id"),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_OK);
    @SuppressWarnings("unchecked")
    Map<String, String> response = objectMapper.readValue(body, Map.class);

    assertThat(response.get("name")).isEqualTo("identifier");

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true",
                host, keyspaceName, tableName, "identifier"),
            HttpStatus.SC_OK);
    ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
    assertThat(column).usingRecursiveComparison().isEqualTo(columnDefinition);
  }

  @Test
  public void updateColumnNotFound() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    ColumnDefinition columnDefinition = new ColumnDefinition("name", "text");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, keyspaceName, tableName, "notFound"),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void updateColumnBadTable() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    ColumnDefinition columnDefinition = new ColumnDefinition("name", "text");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, keyspaceName, "foo", "age"),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void updateColumnBadKeyspace() throws IOException {
    ColumnDefinition columnDefinition = new ColumnDefinition("name", "text");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, "foo", tableName, "age"),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void deleteColumn() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
            host, keyspaceName, tableName, "age"),
        HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void deleteColumnNotFound() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
            host, keyspaceName, tableName, "foo"),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void deleteColumnBadTable() throws IOException {
    createKeyspace(keyspaceName);

    String body =
        RestUtils.delete(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, keyspaceName, "foo", "age"),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void deleteColumnBadKeyspace() throws IOException {
    String body =
        RestUtils.delete(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, "foo", tableName, "age"),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  @Test
  public void deleteColumnPartitionKey() throws IOException {
    createKeyspace(keyspaceName);
    createTable(keyspaceName, tableName);

    String body =
        RestUtils.delete(
            authToken,
            String.format(
                "%s:8082/v2/schemas/keyspaces/%s/tables/%s/columns/%s",
                host, keyspaceName, tableName, "id"),
            HttpStatus.SC_BAD_REQUEST);
    Error response = objectMapper.readValue(body, Error.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription()).isNotEmpty();
  }

  private void createTable(String keyspaceName, String tableName) throws IOException {
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

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s/tables", host, keyspaceName),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_CREATED);
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
            String.format("%s:8082/v2/schemas/keyspaces/%s/tables", host, keyspaceName),
            objectMapper.writeValueAsString(tableAdd),
            HttpStatus.SC_CREATED);

    TableResponse tableResponse =
        objectMapper.readValue(body, new TypeReference<TableResponse>() {});
    assertThat(tableResponse.getName()).isEqualTo(tableName);
  }

  private void createComplexTable(String keyspaceName, String tableName) throws IOException {
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("pk0", "uuid"));
    columnDefinitions.add(new ColumnDefinition("col1", "frozen<map<date, varchar>>"));
    columnDefinitions.add(new ColumnDefinition("col2", "frozen<set<boolean>>"));
    columnDefinitions.add(new ColumnDefinition("col3", "frozen<tuple<duration, inet>>"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("pk0"));
    tableAdd.setPrimaryKey(primaryKey);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s/tables", host, keyspaceName),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_CREATED);
  }

  private void createTableWithClustering(String keyspaceName, String tableName) throws IOException {
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
        String.format("%s:8082/v2/schemas/keyspaces/%s/tables", host, keyspaceName),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_CREATED);
  }

  private void createTableWithMixedClustering(String keyspaceName, String tableName)
      throws IOException {
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("pk0", "int"));
    columnDefinitions.add(new ColumnDefinition("pk1", "text"));
    columnDefinitions.add(new ColumnDefinition("pk2", "int"));
    columnDefinitions.add(new ColumnDefinition("ck0", "int"));
    columnDefinitions.add(new ColumnDefinition("ck1", "text"));
    columnDefinitions.add(new ColumnDefinition("v", "int"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("pk0", "pk1", "pk2"));
    primaryKey.setClusteringKey(Arrays.asList("ck0", "ck1"));
    tableAdd.setPrimaryKey(primaryKey);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s/tables", host, keyspaceName),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_CREATED);
  }

  private void createKeyspace(String keyspaceName) throws IOException {
    String createKeyspaceRequest =
        String.format("{\"name\": \"%s\", \"replicas\": 1}", keyspaceName);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces", host),
        createKeyspaceRequest,
        HttpStatus.SC_CREATED);
  }

  private String setupClusteringTestCase() throws IOException {
    createKeyspace(keyspaceName);
    createTableWithClustering(keyspaceName, tableName);

    String rowIdentifier = "1";
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");
    row.put("expense_id", "1");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");
    row.put("expense_id", "2");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    row = new HashMap<>();
    row.put("id", "2");
    row.put("firstName", "Jane");
    row.put("expense_id", "1");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    row = new HashMap<>();
    row.put("id", "2");
    row.put("firstName", "Jane");
    row.put("expense_id", "1");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    return rowIdentifier;
  }

  private void setupMixedClusteringTestCase() throws IOException {
    createKeyspace(keyspaceName);
    createTableWithMixedClustering(keyspaceName, tableName);

    Map<String, String> row = new HashMap<>();
    row.put("pk0", "1");
    row.put("pk1", "one");
    row.put("pk2", "-1");
    row.put("ck0", "10");
    row.put("ck1", "foo");
    row.put("v", "9");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    row = new HashMap<>();
    row.put("pk0", "1");
    row.put("pk1", "one");
    row.put("pk2", "-1");
    row.put("ck0", "20");
    row.put("ck1", "foo");
    row.put("v", "19");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    row = new HashMap<>();
    row.put("pk0", "2");
    row.put("pk1", "two");
    row.put("pk2", "-2");
    row.put("ck0", "10");
    row.put("ck1", "bar");
    row.put("v", "18");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);
  }
}
