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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.models.Credentials;
import io.stargate.web.models.Changeset;
import io.stargate.web.models.ColumnDefinition;
import io.stargate.web.models.ColumnModel;
import io.stargate.web.models.Filter;
import io.stargate.web.models.PrimaryKey;
import io.stargate.web.models.Query;
import io.stargate.web.models.RowAdd;
import io.stargate.web.models.RowResponse;
import io.stargate.web.models.RowUpdate;
import io.stargate.web.models.Rows;
import io.stargate.web.models.RowsResponse;
import io.stargate.web.models.SuccessResponse;
import io.stargate.web.models.TableAdd;
import io.stargate.web.models.TableResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.osgi.framework.InvalidSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@NotThreadSafe
public class RestApiTest extends BaseOsgiIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(RestApiTest.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static String authToken;
  private static String host = "http://" + stargateHost;
  @Rule public TestName name = new TestName();
  private DataStore dataStore;
  private String keyspace;

  @Before
  public void setup()
      throws InvalidSyntaxException, ExecutionException, InterruptedException, IOException {
    keyspace = "ks_restapitest";

    Persistence persistence = getOsgiService("io.stargate.db.Persistence", Persistence.class);
    ClientState clientState = persistence.newClientState("");
    QueryState queryState = persistence.newQueryState(clientState);
    dataStore = persistence.newDataStore(queryState, null);
    logger.info("{} {} {}", clientState, queryState, dataStore);

    ResultSet result =
        dataStore
            .query()
            .create()
            .keyspace(keyspace)
            .ifNotExists()
            .withReplication("{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
            .andDurableWrites(true)
            .execute();

    dataStore.waitForSchemaAgreement();

    initAuth();
  }

  private void initAuth() throws IOException {
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
  }

  @Test
  public void createTokenBadCreds() throws IOException {
    RestUtils.post(
        "",
        String.format("%s:8081/v1/auth/token/generate", host),
        objectMapper.writeValueAsString(new Credentials("bad", "real_bad")),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void createTokenEmptyBody() throws IOException {
    RestUtils.post(
        "", String.format("%s:8081/v1/auth/token/generate", host), "", HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void getKeyspaces() throws IOException {
    String body =
        RestUtils.get(authToken, String.format("%s:8082/v1/keyspaces", host), HttpStatus.SC_OK);

    List<String> keyspaces = objectMapper.readValue(body, new TypeReference<List<String>>() {});
    assertThat(keyspaces)
        .containsAnyOf(
            "system", "system_auth", "system_distributed", "system_schema", "system_traces");
  }

  @Test
  public void getKeyspacesMissingToken() throws IOException {
    RestUtils.get("", String.format("%s:8082/v1/keyspaces", host), HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void getKeyspacesBadToken() throws IOException {
    RestUtils.get("foo", String.format("%s:8082/v1/keyspaces", host), HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void getTables() throws IOException {
    String body =
        RestUtils.get(
            authToken, String.format("%s:8082/v1/keyspaces/system/tables", host), HttpStatus.SC_OK);

    List<String> keyspaces = objectMapper.readValue(body, new TypeReference<List<String>>() {});
    assertThat(keyspaces)
        .containsAnyOf(
            "IndexInfo",
            "batches",
            "paxos",
            "local",
            "peers_v2",
            "peers",
            "peer_events_v2",
            "peer_events",
            "compaction_history",
            "sstable_activity",
            "size_estimates",
            "available_ranges_v2",
            "available_ranges",
            "transferred_ranges_v2",
            "transferred_ranges",
            "view_builds_in_progress",
            "built_views",
            "prepared_statements",
            "repairs");
  }

  @Test
  public void createTable() throws IOException {
    String tableName = "tbl_createtable_" + System.currentTimeMillis();
    createTable(tableName);
  }

  @Test
  public void getTable() throws IOException {
    String tableName = "tbl_gettable_" + System.currentTimeMillis();
    createTable(tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables/%s", host, keyspace, tableName),
            HttpStatus.SC_OK);

    TableResponse table = objectMapper.readValue(body, new TypeReference<TableResponse>() {});
    assertThat(table.getName()).isEqualTo(tableName);
  }

  @Test
  public void deleteTable() throws IOException {
    String tableName = "tbl_deletetable_" + System.currentTimeMillis();
    createTable(tableName);

    RestUtils.delete(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s", host, keyspace, tableName),
        HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void getRow() throws IOException {
    String tableName = "tbl_getrow_" + System.currentTimeMillis();
    createTable(tableName);

    List<ColumnModel> columns = new ArrayList<>();

    String rowIdentifier = UUID.randomUUID().toString();
    ColumnModel idColumn = new ColumnModel();
    idColumn.setName("id");
    idColumn.setValue(rowIdentifier);
    columns.add(idColumn);

    ColumnModel firstNameColumn = new ColumnModel();
    firstNameColumn.setName("firstName");
    firstNameColumn.setValue("John");
    columns.add(firstNameColumn);
    addRow(tableName, columns);

    String body = getRow(tableName, rowIdentifier);

    RowResponse rowResponse = objectMapper.readValue(body, new TypeReference<RowResponse>() {});
    assertThat(rowResponse.getCount()).isEqualTo(1);
    assertThat(rowResponse.getRows().get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(rowResponse.getRows().get(0).get("firstName")).isEqualTo("John");
  }

  @Test
  public void updateRow() throws IOException {
    String tableName = "tbl_updaterow_" + System.currentTimeMillis();
    createTable(tableName);

    List<ColumnModel> columns = new ArrayList<>();

    String rowIdentifier = UUID.randomUUID().toString();
    ColumnModel idColumn = new ColumnModel();
    idColumn.setName("id");
    idColumn.setValue(rowIdentifier);
    columns.add(idColumn);

    ColumnModel firstNameColumn = new ColumnModel();
    firstNameColumn.setName("firstName");
    firstNameColumn.setValue("John");
    columns.add(firstNameColumn);
    addRow(tableName, columns);

    String row = getRow(tableName, rowIdentifier);

    RowResponse rowResponse = objectMapper.readValue(row, new TypeReference<RowResponse>() {});
    assertThat(rowResponse.getCount()).isEqualTo(1);
    assertThat(rowResponse.getRows().get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(rowResponse.getRows().get(0).get("firstName")).isEqualTo("John");

    RowUpdate rowUpdate = new RowUpdate();
    Changeset firstNameChange = new Changeset();
    firstNameChange.setColumn("firstName");
    firstNameChange.setValue("Fred");
    rowUpdate.setChangeset(Collections.singletonList(firstNameChange));

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v1/keyspaces/%s/tables/%s/rows/%s",
                host, keyspace, tableName, rowIdentifier),
            objectMapper.writeValueAsString(rowUpdate),
            HttpStatus.SC_OK);

    SuccessResponse successResponse = objectMapper.readValue(body, SuccessResponse.class);
    assertThat(successResponse.getSuccess()).isTrue();

    rowResponse =
        objectMapper.readValue(
            getRow(tableName, rowIdentifier), new TypeReference<RowResponse>() {});
    assertThat(rowResponse.getCount()).isEqualTo(1);
    assertThat(rowResponse.getRows().get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(rowResponse.getRows().get(0).get("firstName")).isEqualTo("Fred");
  }

  @Test
  public void getRowSystemLocal() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v1/keyspaces/%s/tables/%s/rows/%s", host, "system", "local", "local"),
            HttpStatus.SC_OK);

    RowResponse rowResponse = objectMapper.readValue(body, new TypeReference<RowResponse>() {});
    assertThat(rowResponse.getCount()).isEqualTo(1);
    assertThat(rowResponse.getRows().get(0).get("cluster_name")).isEqualTo("Test Cluster");
  }

  @Test
  public void getRowBadRequest() throws IOException {
    RestUtils.get(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows/peer", host, "system", "peers"),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void getAllRows() throws IOException {
    String tableName = "tbl_getallrows_" + System.currentTimeMillis();
    createTable(tableName);

    List<ColumnModel> columns = new ArrayList<>();
    ColumnModel idColumn = new ColumnModel();
    idColumn.setName("id");
    idColumn.setValue(UUID.randomUUID().toString());
    columns.add(idColumn);

    ColumnModel firstNameColumn = new ColumnModel();
    firstNameColumn.setName("firstName");
    firstNameColumn.setValue("John");
    columns.add(firstNameColumn);
    addRow(tableName, columns);

    columns = new ArrayList<>();
    idColumn = new ColumnModel();
    idColumn.setName("id");
    idColumn.setValue(UUID.randomUUID().toString());
    columns.add(idColumn);

    firstNameColumn = new ColumnModel();
    firstNameColumn.setName("firstName");
    firstNameColumn.setValue("Jane");
    columns.add(firstNameColumn);
    addRow(tableName, columns);

    columns = new ArrayList<>();
    idColumn = new ColumnModel();
    idColumn.setName("id");
    idColumn.setValue(UUID.randomUUID().toString());
    columns.add(idColumn);

    firstNameColumn = new ColumnModel();
    firstNameColumn.setName("firstName");
    firstNameColumn.setValue("Alice");
    columns.add(firstNameColumn);
    addRow(tableName, columns);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v1/keyspaces/%s/tables/%s/rows?pageSize=2", host, keyspace, tableName),
            HttpStatus.SC_OK);

    Rows rows = objectMapper.readValue(body, new TypeReference<Rows>() {});
    assertThat(rows.getCount()).isEqualTo(2);
    assertThat(rows.getPageState()).isNotNull();
  }

  @Test
  public void getAllRowsNoSize() throws IOException {
    String tableName = "tbl_getallrowsnosize_" + System.currentTimeMillis();
    createTable(tableName);

    List<ColumnModel> columns = new ArrayList<>();
    ColumnModel idColumn = new ColumnModel();
    idColumn.setName("id");
    idColumn.setValue(UUID.randomUUID().toString());
    columns.add(idColumn);

    ColumnModel firstNameColumn = new ColumnModel();
    firstNameColumn.setName("firstName");
    firstNameColumn.setValue("John");
    columns.add(firstNameColumn);
    addRow(tableName, columns);

    columns = new ArrayList<>();
    idColumn = new ColumnModel();
    idColumn.setName("id");
    idColumn.setValue(UUID.randomUUID().toString());
    columns.add(idColumn);

    firstNameColumn = new ColumnModel();
    firstNameColumn.setName("firstName");
    firstNameColumn.setValue("Jane");
    columns.add(firstNameColumn);
    addRow(tableName, columns);

    columns = new ArrayList<>();
    idColumn = new ColumnModel();
    idColumn.setName("id");
    idColumn.setValue(UUID.randomUUID().toString());
    columns.add(idColumn);

    firstNameColumn = new ColumnModel();
    firstNameColumn.setName("firstName");
    firstNameColumn.setValue("Alice");
    columns.add(firstNameColumn);
    addRow(tableName, columns);

    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows", host, keyspace, tableName),
            HttpStatus.SC_OK);

    Rows rows = objectMapper.readValue(body, new TypeReference<Rows>() {});
    assertThat(rows.getCount()).isEqualTo(3);
  }

  @Test
  public void addRow() throws IOException {
    String tableName = "tbl_addrow_" + System.currentTimeMillis();
    createTable(tableName);

    RowAdd rowAdd = new RowAdd();

    List<ColumnModel> columns = new ArrayList<>();

    String rowIdentifier = UUID.randomUUID().toString();
    ColumnModel idColumn = new ColumnModel();
    idColumn.setName("id");
    idColumn.setValue(rowIdentifier);
    columns.add(idColumn);

    ColumnModel lastNameColumn = new ColumnModel();
    lastNameColumn.setName("lastName");
    lastNameColumn.setValue("Doe");
    columns.add(lastNameColumn);

    ColumnModel firstNameColumn = new ColumnModel();
    firstNameColumn.setName("firstName");
    firstNameColumn.setValue("John");
    columns.add(firstNameColumn);

    rowAdd.setColumns(columns);

    String body =
        RestUtils.post(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows", host, keyspace, tableName),
            objectMapper.writeValueAsString(rowAdd),
            HttpStatus.SC_CREATED);

    RowsResponse rowsResponse = objectMapper.readValue(body, new TypeReference<RowsResponse>() {});
    assertThat(rowsResponse.getRowsModified()).isEqualTo(1);
    assertThat(rowsResponse.getSuccess()).isTrue();

    getRow(tableName, rowIdentifier);
  }

  @Test
  public void query() throws IOException {
    String tableName = "tbl_query_" + System.currentTimeMillis();
    createTable(tableName);

    List<ColumnModel> columns = new ArrayList<>();

    String rowIdentifier = UUID.randomUUID().toString();
    ColumnModel idColumn = new ColumnModel();
    idColumn.setName("id");
    idColumn.setValue(rowIdentifier);
    columns.add(idColumn);

    ColumnModel lastNameColumn = new ColumnModel();
    lastNameColumn.setName("lastName");
    lastNameColumn.setValue("Doe");
    columns.add(lastNameColumn);

    ColumnModel firstNameColumn = new ColumnModel();
    firstNameColumn.setName("firstName");
    firstNameColumn.setValue("John");
    columns.add(firstNameColumn);
    addRow(tableName, columns);

    Query query = new Query();
    query.setColumnNames(Arrays.asList("id", "firstName"));

    List<Filter> filters = new ArrayList<>();

    Filter filter = new Filter();
    filter.setColumnName("id");
    filter.setOperator(Filter.Operator.eq);
    filter.setValue(Collections.singletonList(rowIdentifier));
    filters.add(filter);

    query.setFilters(filters);

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspace, tableName),
            objectMapper.writeValueAsString(query),
            HttpStatus.SC_OK);

    RowResponse rowResponse = objectMapper.readValue(body, new TypeReference<RowResponse>() {});
    assertThat(rowResponse.getCount()).isEqualTo(1);
    assertThat(rowResponse.getRows().get(0).get("id")).isEqualTo(rowIdentifier);
  }

  @Test
  public void queryWithPaging() throws IOException {
    String tableName = "tbl_query_" + System.currentTimeMillis();
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("name", "text"));
    columnDefinitions.add(new ColumnDefinition("date", "timestamp"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("name"));
    primaryKey.setClusteringKey(Collections.singletonList("date"));
    tableAdd.setPrimaryKey(primaryKey);

    String body =
        RestUtils.post(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables", host, keyspace),
            objectMapper.writeValueAsString(tableAdd),
            HttpStatus.SC_CREATED);

    List<ColumnModel> columns = new ArrayList<>();
    ColumnModel nameColumn = new ColumnModel();
    nameColumn.setName("name");
    nameColumn.setValue("John Doe");
    columns.add(nameColumn);

    ColumnModel dateColumn = new ColumnModel();
    dateColumn.setName("date");
    dateColumn.setValue("2020-08-08T18:48:31.020Z");
    columns.add(dateColumn);
    addRow(tableName, columns);

    columns = new ArrayList<>();
    nameColumn = new ColumnModel();
    nameColumn.setName("name");
    nameColumn.setValue("John Doe");
    columns.add(nameColumn);

    dateColumn = new ColumnModel();
    dateColumn.setName("date");
    dateColumn.setValue("2020-08-10T18:48:31.020Z");
    columns.add(dateColumn);
    addRow(tableName, columns);

    Query query = new Query();
    query.setColumnNames(Arrays.asList("name", "date"));

    List<Filter> filters = new ArrayList<>();

    Filter filter = new Filter();
    filter.setColumnName("name");
    filter.setOperator(Filter.Operator.eq);
    filter.setValue(Collections.singletonList("John Doe"));
    filters.add(filter);

    filter = new Filter();
    filter.setColumnName("date");
    filter.setOperator(Filter.Operator.gt);
    filter.setValue(Collections.singletonList("2020-08-08T18:48:31.020Z"));
    filters.add(filter);

    query.setFilters(filters);
    query.setPageSize(1);

    body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspace, tableName),
            objectMapper.writeValueAsString(query),
            HttpStatus.SC_OK);

    Rows rows = objectMapper.readValue(body, new TypeReference<Rows>() {});
    assertThat(rows.getCount()).isEqualTo(1);
    assertThat(rows.getRows().get(0).get("date")).isEqualTo("2020-08-10T18:48:31.020Z");
  }

  @Test
  public void queryWithFilterMissingColumnName() throws IOException {
    String tableName = "tbl_query_" + System.currentTimeMillis();
    createTable(tableName);

    Query query = new Query();
    List<Filter> filters = new ArrayList<>();

    Filter filter = new Filter();
    filter.setOperator(Filter.Operator.eq);
    filter.setValue(Collections.singletonList("John Doe"));
    filters.add(filter);

    query.setFilters(filters);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspace, tableName),
        objectMapper.writeValueAsString(query),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void queryWithFilterMissingOperator() throws IOException {
    String tableName = "tbl_query_" + System.currentTimeMillis();
    createTable(tableName);

    Query query = new Query();
    List<Filter> filters = new ArrayList<>();

    Filter filter = new Filter();
    filter.setColumnName("firstName");
    filter.setValue(Collections.singletonList("John Doe"));
    filters.add(filter);

    query.setFilters(filters);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspace, tableName),
        objectMapper.writeValueAsString(query),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void queryWithFilterEmptyValueList() throws IOException {
    String tableName = "tbl_query_" + System.currentTimeMillis();
    createTable(tableName);

    Query query = new Query();
    List<Filter> filters = new ArrayList<>();

    Filter filter = new Filter();
    filter.setColumnName("firstName");
    filter.setOperator(Filter.Operator.eq);
    filter.setValue(Collections.emptyList());
    filters.add(filter);

    query.setFilters(filters);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspace, tableName),
        objectMapper.writeValueAsString(query),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void queryWithFilterMissingValueList() throws IOException {
    String tableName = "tbl_query_" + System.currentTimeMillis();
    createTable(tableName);

    Query query = new Query();
    List<Filter> filters = new ArrayList<>();

    Filter filter = new Filter();
    filter.setColumnName("firstName");
    filter.setOperator(Filter.Operator.eq);
    filters.add(filter);

    query.setFilters(filters);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspace, tableName),
        objectMapper.writeValueAsString(query),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void queryWithEmptyFilter() throws IOException {
    String tableName = "tbl_query_" + System.currentTimeMillis();
    createTable(tableName);

    Query query = new Query();
    List<Filter> filters = new ArrayList<>();
    filters.add(new Filter());

    query.setFilters(filters);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspace, tableName),
        objectMapper.writeValueAsString(query),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void queryWithEmptyFilters() throws IOException {
    String tableName = "tbl_query_" + System.currentTimeMillis();
    createTable(tableName);

    Query query = new Query();

    query.setFilters(Collections.emptyList());

    RestUtils.post(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspace, tableName),
        objectMapper.writeValueAsString(query),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void queryWithMissingFilter() throws IOException {
    String tableName = "tbl_query_" + System.currentTimeMillis();
    createTable(tableName);

    Query query = new Query();

    RestUtils.post(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspace, tableName),
        objectMapper.writeValueAsString(query),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void getColumns() throws IOException {
    String tableName = "tbl_getcolumns_" + System.currentTimeMillis();
    createTable(tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables/%s/columns", host, keyspace, tableName),
            HttpStatus.SC_OK);

    List<ColumnDefinition> columnDefinitions =
        objectMapper.readValue(body, new TypeReference<List<ColumnDefinition>>() {});
    assertThat(columnDefinitions.size()).isEqualTo(3);
    columnDefinitions.sort(Comparator.comparing(ColumnDefinition::getName));
    assertThat(columnDefinitions.get(0).getName()).isEqualTo("firstName");
  }

  @Test
  public void getColumn() throws IOException {
    String tableName = "tbl_getcolumn_" + System.currentTimeMillis();
    createTable(tableName);

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v1/keyspaces/%s/tables/%s/columns/firstName", host, keyspace, tableName),
            HttpStatus.SC_OK);

    ColumnDefinition columnDefinition =
        objectMapper.readValue(body, new TypeReference<ColumnDefinition>() {});
    assertThat(columnDefinition.getName()).isEqualTo("firstName");
    assertThat(columnDefinition.getTypeDefinition()).isEqualTo("Varchar");
  }

  @Test
  public void addColumn() throws IOException {
    String tableName = "tbl_addcolumn_" + System.currentTimeMillis();
    createTable(tableName);

    ColumnDefinition columnDefinition = new ColumnDefinition("middleName", "text");

    createColumn(tableName, columnDefinition);
  }

  @Test
  public void deleteColumn() throws IOException {
    String tableName = "tbl_deletecolumn_" + System.currentTimeMillis();
    createTable(tableName);

    createColumn(tableName, new ColumnDefinition("middleName", "text"));

    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v1/keyspaces/%s/tables/%s/columns/middleName", host, keyspace, tableName),
        HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void ping() throws IOException {
    assertThat(RestUtils.get(authToken, String.format("%s:8082/", host), HttpStatus.SC_OK))
        .isEqualTo("It's Alive");
  }

  @Test
  public void health() throws IOException {
    assertThat(RestUtils.get(authToken, String.format("%s:8082/health", host), HttpStatus.SC_OK))
        .isEqualTo("UP");
  }

  private void createTable(String tableName) throws IOException {
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(tableName);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("id", "uuid"));
    columnDefinitions.add(new ColumnDefinition("lastName", "text"));
    columnDefinitions.add(new ColumnDefinition("firstName", "text"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("id"));
    tableAdd.setPrimaryKey(primaryKey);

    String body =
        RestUtils.post(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables", host, keyspace),
            objectMapper.writeValueAsString(tableAdd),
            HttpStatus.SC_CREATED);

    SuccessResponse successResponse =
        objectMapper.readValue(body, new TypeReference<SuccessResponse>() {});
    assertThat(successResponse.getSuccess()).isTrue();
  }

  private String getRow(String tableName, String rowIdentifier) throws IOException {
    return RestUtils.get(
        authToken,
        String.format(
            "%s:8082/v1/keyspaces/%s/tables/%s/rows/%s", host, keyspace, tableName, rowIdentifier),
        HttpStatus.SC_OK);
  }

  private void addRow(String tableName, List<ColumnModel> columns) throws IOException {
    RowAdd rowAdd = new RowAdd();
    rowAdd.setColumns(columns);

    String body =
        RestUtils.post(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows", host, keyspace, tableName),
            objectMapper.writeValueAsString(rowAdd),
            HttpStatus.SC_CREATED);

    RowsResponse rowsResponse = objectMapper.readValue(body, new TypeReference<RowsResponse>() {});
    assertThat(rowsResponse.getRowsModified()).isEqualTo(1);
    assertThat(rowsResponse.getSuccess()).isTrue();
  }

  private void createColumn(String tableName, ColumnDefinition columnDefinition)
      throws IOException {
    String body =
        RestUtils.post(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables/%s/columns", host, keyspace, tableName),
            objectMapper.writeValueAsString(columnDefinition),
            HttpStatus.SC_CREATED);

    SuccessResponse successResponse =
        objectMapper.readValue(body, new TypeReference<SuccessResponse>() {});
    assertThat(successResponse.getSuccess()).isTrue();
  }
}
