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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.models.Credentials;
import io.stargate.web.models.ColumnDefinition;
import io.stargate.web.models.Error;
import io.stargate.web.models.GetResponseWrapper;
import io.stargate.web.models.Keyspace;
import io.stargate.web.models.PrimaryKey;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.models.TableAdd;
import io.stargate.web.models.TableOptions;
import io.stargate.web.models.TableResponse;
import net.jcip.annotations.NotThreadSafe;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
@NotThreadSafe
public class RestApiv2Test extends BaseOsgiIntegrationTest
{
    private static final Logger logger = LoggerFactory.getLogger(RestApiv2Test.class);

    @Rule
    public TestName name = new TestName();

    private String keyspaceName;
    private String tableName;
    private static String authToken;
    private static String host = "http://" + stargateHost;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setup() throws IOException {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        String body = RestUtils.post("", String.format("%s:8081/v1/auth/token/generate", host),
                objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
                HttpStatus.SC_OK);

        AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
        authToken = authTokenResponse.getAuthToken();
        assertThat(authToken).isNotNull();

        String testName = name.getMethodName();
        if (testName.contains("[")) {
            testName = testName.substring(0, testName.indexOf("["));
        }
        keyspaceName = "ks_" + testName + "_" + System.currentTimeMillis();
        tableName = "tbl_" + testName + "_" + System.currentTimeMillis();
    }

    @Test
    public void getKeyspaces() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces", host), HttpStatus.SC_OK);

        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        List<Keyspace> keyspaces = objectMapper.convertValue(response.getData(), new TypeReference<List<Keyspace>>() { });
        assertThat(keyspaces)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingFieldByField(new Keyspace("system", null)));
    }

    @Test
    public void getKeyspacesPretty() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces?pretty=true", host), HttpStatus.SC_OK);

        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        List<Keyspace> keyspaces = objectMapper.convertValue(response.getData(), new TypeReference<List<Keyspace>>() { });
        assertThat(keyspaces)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingFieldByField(new Keyspace("system_schema", null)));
    }

    @Test
    public void getKeyspacesRaw() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces?raw=true", host), HttpStatus.SC_OK);

        List<Keyspace> keyspaces = objectMapper.readValue(body, new TypeReference<List<Keyspace>>() { });
        assertThat(keyspaces)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingFieldByField(new Keyspace("system_schema", null)));
    }

    @Test
    public void getKeyspacesPrettyAndRaw() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces?pretty=true&raw=true", host), HttpStatus.SC_OK);

        List<Keyspace> keyspaces = objectMapper.readValue(body, new TypeReference<List<Keyspace>>() { });
        assertThat(keyspaces)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingFieldByField(new Keyspace("system_schema", null)));
    }

    @Test
    public void getKeyspace() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system", host), HttpStatus.SC_OK);

        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        Keyspace keyspace = objectMapper.convertValue(response.getData(), Keyspace.class);

        assertThat(keyspace).isEqualToComparingFieldByField(new Keyspace("system", null));
    }

    @Test
    public void getKeyspacePretty() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system?pretty=true", host), HttpStatus.SC_OK);

        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        Keyspace keyspace = objectMapper.convertValue(response.getData(), Keyspace.class);

        assertThat(keyspace).isEqualToComparingFieldByField(new Keyspace("system", null));
    }

    @Test
    public void getKeyspaceRaw() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system?raw=true", host), HttpStatus.SC_OK);

        Keyspace keyspace = objectMapper.readValue(body, Keyspace.class);

        assertThat(keyspace).isEqualToComparingFieldByField(new Keyspace("system", null));
    }

    @Test
    public void getKeyspacePrettyAndRaw() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system?pretty=true&raw=true", host), HttpStatus.SC_OK);

        Keyspace keyspace = objectMapper.readValue(body, Keyspace.class);

        assertThat(keyspace).isEqualToComparingFieldByField(new Keyspace("system", null));
    }

    @Test
    public void getKeyspaceNotFound() throws IOException {
        RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/ks_not_found", host), HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void createKeyspace() throws IOException {
        String keyspaceName = "ks_createkeyspace_" + System.currentTimeMillis();
        createKeyspace(keyspaceName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s?raw=true", host, keyspaceName), HttpStatus.SC_OK);

        Keyspace keyspace = objectMapper.readValue(body, Keyspace.class);

        assertThat(keyspace).isEqualToComparingFieldByField(new Keyspace(keyspaceName, null));
    }

    @Test
    public void deleteKeyspace() throws IOException {
        String keyspaceName = "ks_createkeyspace_" + System.currentTimeMillis();
        createKeyspace(keyspaceName);

        RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s", host, keyspaceName), HttpStatus.SC_OK);

        RestUtils.delete(authToken, String.format("%s:8082/schemas/keyspaces/%s", host, keyspaceName), HttpStatus.SC_NO_CONTENT);

        RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s", host, keyspaceName), HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void getTables() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system/tables", host), HttpStatus.SC_OK);

        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        List<TableResponse> tables = objectMapper.convertValue(response.getData(), new TypeReference<List<TableResponse>>() { });

        assertThat(tables.size()).isGreaterThan(5);
        assertThat(tables)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingOnlyGivenFields(new TableResponse("local", "system", null, null, null), "name", "keyspace"));
    }

    @Test
    public void getTablesPretty() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system/tables?pretty=true", host), HttpStatus.SC_OK);

        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        List<TableResponse> tables = objectMapper.convertValue(response.getData(), new TypeReference<List<TableResponse>>() { });

        assertThat(tables.size()).isGreaterThan(5);
        assertThat(tables)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingOnlyGivenFields(new TableResponse("local", "system", null, null, null), "name", "keyspace"));
    }

    @Test
    public void getTablesRaw() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system/tables?raw=true", host), HttpStatus.SC_OK);

        List<TableResponse> tables = objectMapper.readValue(body, new TypeReference<List<TableResponse>>() { });

        assertThat(tables.size()).isGreaterThan(5);
        assertThat(tables)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingOnlyGivenFields(new TableResponse("local", "system", null, null, null), "name", "keyspace"));
    }

    @Test
    public void getTablesPrettyAndRaw() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system/tables?pretty=true&raw=true", host), HttpStatus.SC_OK);

        List<TableResponse> tables = objectMapper.readValue(body, new TypeReference<List<TableResponse>>() { });

        assertThat(tables.size()).isGreaterThan(5);
        assertThat(tables)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingOnlyGivenFields(new TableResponse("local", "system", null, null, null), "name", "keyspace"));
    }

    @Test
    public void getTable() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system/tables/local", host), HttpStatus.SC_OK);

        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        TableResponse table = objectMapper.convertValue(response.getData(), TableResponse.class);
        assertThat(table.getKeyspace()).isEqualTo("system");
        assertThat(table.getName()).isEqualTo("local");
        assertThat(table.getColumnDefinitions()).isNotNull();
    }

    @Test
    public void getTableRaw() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system/tables/local?raw=true", host), HttpStatus.SC_OK);

        TableResponse table = objectMapper.readValue(body, TableResponse.class);
        assertThat(table.getKeyspace()).isEqualTo("system");
        assertThat(table.getName()).isEqualTo("local");
        assertThat(table.getColumnDefinitions()).isNotNull();
    }

    @Test
    public void getTablePretty() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system/tables/local?pretty=true", host), HttpStatus.SC_OK);

        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        TableResponse table = objectMapper.convertValue(response.getData(), TableResponse.class);
        assertThat(table.getKeyspace()).isEqualTo("system");
        assertThat(table.getName()).isEqualTo("local");
        assertThat(table.getColumnDefinitions()).isNotNull();
    }

    @Test
    public void getTablePrettyAndRaw() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system/tables/local?pretty=true&raw=true", host), HttpStatus.SC_OK);

        TableResponse table = objectMapper.readValue(body, TableResponse.class);
        assertThat(table.getKeyspace()).isEqualTo("system");
        assertThat(table.getName()).isEqualTo("local");
        assertThat(table.getColumnDefinitions()).isNotNull();
    }

    @Test
    public void getTableNotFound() throws IOException {
        RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/system/tables/tbl_not_found", host), HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void createTable() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s?raw=true", host, keyspaceName, tableName), HttpStatus.SC_OK);

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

        RestUtils.put(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(tableUpdate), HttpStatus.SC_CREATED);
    }

    @Test
    public void deleteTable() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        RestUtils.delete(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s", host, keyspaceName, tableName), HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void getRowsWithQuery() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s?where=%s", host, keyspaceName, tableName, whereClause),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
    }

    @Test
    public void getRowsWithQueryAndPaging() throws IOException {
        String rowIdentifier = setupClusteringTestCase();

        String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s?where=%s&page-size=1", host, keyspaceName, tableName, whereClause),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(getResponseWrapper.getCount()).isEqualTo(1);
        assertThat(getResponseWrapper.getPageState()).isNotEmpty();
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
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

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s?where=%s&raw=true", host, keyspaceName, tableName, whereClause),
                HttpStatus.SC_OK);

        List<Map<String, Object>> data = objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() { });
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
    }

    @Test
    public void getRowsWithQueryAndSort() throws IOException {
        String rowIdentifier = setupClusteringTestCase();

        String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s?where=%s&sort={\"expense_id\":\"desc\"}", host, keyspaceName, tableName, whereClause),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(getResponseWrapper.getCount()).isEqualTo(2);
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
        assertThat(data.get(0).get("expense_id")).isEqualTo(2);
    }

    @Test
    public void getRowsWithQueryRawAndSort() throws IOException {
        String rowIdentifier = setupClusteringTestCase();

        String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
        String body = RestUtils.get(authToken,
                String.format("%s:8082/v2/keyspaces/%s/%s?where=%s&sort={\"expense_id\":\"desc\"}&raw=true", host, keyspaceName, tableName, whereClause),
                HttpStatus.SC_OK);

        List<Map<String, Object>> data = objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() { });
        assertThat(data.size()).isEqualTo(2);
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
        assertThat(data.get(0).get("expense_id")).isEqualTo(2);
    }

    @Test
    public void getRowsWithQueryPretty() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s?where=%s&pretty=tru", host, keyspaceName, tableName, whereClause),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
    }

    @Test
    public void getRowsWithNotFound() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        String whereClause = "{\"id\":{\"$eq\":\"f0014be3-b69f-4884-b9a6-49765fb40df3\"}}";
        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s?where=%s", host, keyspaceName, tableName, whereClause),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(getResponseWrapper.getCount()).isEqualTo(0);
        assertThat(data).isEmpty();
    }

    @Test
    public void getRows() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);
        
        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
    }

    @Test
    public void getRowsSort() throws IOException {
        String rowIdentifier = setupClusteringTestCase();

        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s?sort={\"expense_id\":\"desc\"}", host, keyspaceName, tableName, rowIdentifier),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(getResponseWrapper.getCount()).isEqualTo(2);
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
        assertThat(data.get(0).get("expense_id")).isEqualTo(2);
    }

    @Test
    public void getRowsPaging() throws IOException {
        String rowIdentifier = setupClusteringTestCase();

        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s?page-size=1", host, keyspaceName, tableName, rowIdentifier),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(getResponseWrapper.getCount()).isEqualTo(1);
        assertThat(getResponseWrapper.getPageState()).isNotEmpty();
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
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

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, "f0014be3-b69f-4884-b9a6-49765fb40df3"),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
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

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s?raw=true", host, keyspaceName, tableName, rowIdentifier),
                HttpStatus.SC_OK);

        List<Map<String, Object>> data = objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() { });
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
    }

    @Test
    public void getRowsRawAndSort() throws IOException {
        String rowIdentifier = setupClusteringTestCase();

        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s?sort={\"expense_id\": \"desc\"}&raw=true", host, keyspaceName, tableName, rowIdentifier),
                HttpStatus.SC_OK);

        List<Map<String, Object>> data = objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() { });
        assertThat(data.size()).isEqualTo(2);
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
        assertThat(data.get(0).get("expense_id")).isEqualTo(2);
    }

    @Test
    public void getRowsPretty() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
    }

    @Test
    public void getRowsPartitionKeyOnly() throws IOException {
        String rowIdentifier = setupClusteringTestCase();

        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(getResponseWrapper.getCount()).isEqualTo(2);
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
        assertThat(data.get(0).get("expense_id")).isEqualTo(1);
    }

    @Test
    public void getRowsPartitionAndClusterKeys() throws IOException {
        String rowIdentifier = setupClusteringTestCase();

        String body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s/2", host, keyspaceName, tableName, rowIdentifier),
                HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(getResponseWrapper.getCount()).isEqualTo(1);
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("John");
        assertThat(data.get(0).get("expense_id")).isEqualTo(2);
    }

    @Test
    public void addRow() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");

        String body = RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        Map<String, Object> rowResponse = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() { });
        assertThat(rowResponse.get("id")).isEqualTo(rowIdentifier);
    }

    @Test
    public void addRowPretty() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");
        row.put("lastName", "Doe");
        row.put("age", "20");

        String body = RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s?pretty=true", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        Map<String, Object> rowResponse = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() { });
        assertThat(rowResponse.get("id")).isEqualTo(rowIdentifier);
    }

    @Test
    public void updateRow() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        Map<String, String> rowUpdate = new HashMap<>();
        rowUpdate.put("firstName", "Robert");
        rowUpdate.put("lastName", "Plant");

        String body = RestUtils.put(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
                objectMapper.writeValueAsString(rowUpdate), HttpStatus.SC_OK);

        ResponseWrapper responseWrapper = objectMapper.readValue(body, ResponseWrapper.class);
        Map<String, String> data = objectMapper.convertValue(responseWrapper.getData(), Map.class);

        assertThat(data).containsAllEntriesOf(rowUpdate);
    }

    @Test
    public void updateRowPretty() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        Map<String, String> rowUpdate = new HashMap<>();
        rowUpdate.put("firstName", "Robert");
        rowUpdate.put("lastName", "Plant");

        String body = RestUtils.put(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s?pretty=true", host, keyspaceName, tableName, rowIdentifier),
                objectMapper.writeValueAsString(rowUpdate), HttpStatus.SC_OK);

        ResponseWrapper responseWrapper = objectMapper.readValue(body, ResponseWrapper.class);
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

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        Map<String, String> rowUpdate = new HashMap<>();
        rowUpdate.put("firstName", "Robert");
        rowUpdate.put("lastName", "Plant");

        String body = RestUtils.put(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s?raw=true", host, keyspaceName, tableName, rowIdentifier),
                objectMapper.writeValueAsString(rowUpdate), HttpStatus.SC_OK);

        Map<String, String> data = objectMapper.readValue(body, Map.class);

        assertThat(data).containsAllEntriesOf(rowUpdate);
    }

    @Test
    public void updateRowPrettyAndRaw() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        Map<String, String> rowUpdate = new HashMap<>();
        rowUpdate.put("firstName", "Robert");
        rowUpdate.put("lastName", "Plant");

        String body = RestUtils.put(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s?pretty=true&raw=true", host, keyspaceName, tableName, rowIdentifier),
                objectMapper.writeValueAsString(rowUpdate), HttpStatus.SC_OK);

        Map<String, String> data = objectMapper.readValue(body, Map.class);

        assertThat(data).containsAllEntriesOf(rowUpdate);
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

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        Map<String, String> rowUpdate = new HashMap<>();
        rowUpdate.put("firstName", "Jane");

        String body = RestUtils.patch(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
                objectMapper.writeValueAsString(rowUpdate), HttpStatus.SC_OK);
        ResponseWrapper responseWrapper = objectMapper.readValue(body, ResponseWrapper.class);
        Map<String, String> patchData = objectMapper.convertValue(responseWrapper.getData(), Map.class);

        assertThat(patchData).containsAllEntriesOf(rowUpdate);

        body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier), HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("Jane");
        assertThat(data.get(0).get("lastName")).isEqualTo("Doe");
    }

    @Test
    public void patchRowPretty() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");
        row.put("lastName", "Doe");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        Map<String, String> rowUpdate = new HashMap<>();
        rowUpdate.put("firstName", "Jane");

        String body = RestUtils.patch(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s?pretty=true", host, keyspaceName, tableName, rowIdentifier),
                objectMapper.writeValueAsString(rowUpdate), HttpStatus.SC_OK);
        ResponseWrapper responseWrapper = objectMapper.readValue(body, ResponseWrapper.class);
        Map<String, String> patchData = objectMapper.convertValue(responseWrapper.getData(), Map.class);

        assertThat(patchData).containsAllEntriesOf(rowUpdate);

        body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier), HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
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

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        Map<String, String> rowUpdate = new HashMap<>();
        rowUpdate.put("firstName", "Jane");

        String body = RestUtils.patch(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s?raw=true", host, keyspaceName, tableName, rowIdentifier),
                objectMapper.writeValueAsString(rowUpdate), HttpStatus.SC_OK);
        Map<String, String> patchData = objectMapper.readValue(body, Map.class);

        assertThat(patchData).containsAllEntriesOf(rowUpdate);

        body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier), HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
        assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
        assertThat(data.get(0).get("firstName")).isEqualTo("Jane");
        assertThat(data.get(0).get("lastName")).isEqualTo("Doe");
    }

    @Test
    public void patchRowPrettyAndRaw() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");
        row.put("lastName", "Doe");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        Map<String, String> rowUpdate = new HashMap<>();
        rowUpdate.put("firstName", "Jane");

        String body = RestUtils.patch(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s?pretty=true&raw=true", host, keyspaceName, tableName, rowIdentifier),
                objectMapper.writeValueAsString(rowUpdate), HttpStatus.SC_OK);
        Map<String, String> patchData = objectMapper.readValue(body, Map.class);

        assertThat(patchData).containsAllEntriesOf(rowUpdate);

        body = RestUtils.get(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier), HttpStatus.SC_OK);

        GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
        List<Map<String, Object>> data = objectMapper.convertValue(getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() { });
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

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        RestUtils.delete(authToken, String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier), HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void getColumns() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, tableName), HttpStatus.SC_OK);
        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        List<ColumnDefinition> columns = objectMapper.convertValue(response.getData(), new TypeReference<List<ColumnDefinition>>() { });
        assertThat(columns)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingFieldByField(new ColumnDefinition("id", "Uuid", false)));
    }

    @Test
    public void getColumnsRaw() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns?raw=true", host, keyspaceName, tableName), HttpStatus.SC_OK);
        List<ColumnDefinition> columns = objectMapper.readValue(body, new TypeReference<List<ColumnDefinition>>() { });
        assertThat(columns)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingFieldByField(new ColumnDefinition("id", "Uuid", false)));
    }

    @Test
    public void getColumnsPretty() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns?pretty=true", host, keyspaceName, tableName), HttpStatus.SC_OK);
        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        List<ColumnDefinition> columns = objectMapper.convertValue(response.getData(), new TypeReference<List<ColumnDefinition>>() { });
        assertThat(columns)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingFieldByField(new ColumnDefinition("firstName", "Varchar", false)));
    }

    @Test
    public void getColumnsPrettyAndRaw() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns?pretty=true&raw=true", host, keyspaceName, tableName), HttpStatus.SC_OK);
        List<ColumnDefinition> columns = objectMapper.readValue(body, new TypeReference<List<ColumnDefinition>>() { });
        assertThat(columns)
                .anySatisfy(value -> assertThat(value)
                        .isEqualToComparingFieldByField(new ColumnDefinition("age", "Int", false)));
    }

    @Test
    public void getColumnsBadTable() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, "foo"), HttpStatus.SC_BAD_REQUEST);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void getColumnsBadKeyspace() throws IOException {
        createKeyspace(keyspaceName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns", host, "foo", tableName), HttpStatus.SC_BAD_REQUEST);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void getColumn() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, keyspaceName, tableName, "age"), HttpStatus.SC_OK);
        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        ColumnDefinition column = objectMapper.convertValue(response.getData(), ColumnDefinition.class);
        assertThat(column).isEqualToComparingFieldByField(new ColumnDefinition("age", "Int", false));
    }

    @Test
    public void getColumnNotFound() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, keyspaceName, tableName, "foo"), HttpStatus.SC_NOT_FOUND);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_NOT_FOUND);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void getColumnRaw() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true", host, keyspaceName, tableName, "age"), HttpStatus.SC_OK);
        ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
        assertThat(column).isEqualToComparingFieldByField(new ColumnDefinition("age", "Int", false));
    }

    @Test
    public void getColumnPretty() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s?pretty=true", host, keyspaceName, tableName, "age"), HttpStatus.SC_OK);
        ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
        ColumnDefinition column = objectMapper.convertValue(response.getData(), ColumnDefinition.class);
        assertThat(column).isEqualToComparingFieldByField(new ColumnDefinition("age", "Int", false));
    }

    @Test
    public void getColumnPrettyAndRaw() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s?pretty=true&raw=true", host, keyspaceName, tableName, "age"), HttpStatus.SC_OK);
        ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
        assertThat(column).isEqualToComparingFieldByField(new ColumnDefinition("age", "Int", false));
    }

    @Test
    public void getColumnBadTable() throws IOException {
        createKeyspace(keyspaceName);

        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, keyspaceName, "foo", "age"), HttpStatus.SC_BAD_REQUEST);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void getColumnBadKeyspace() throws IOException {
        String body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, "foo", tableName, "age"), HttpStatus.SC_BAD_REQUEST);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void addColumn() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        ColumnDefinition columnDefinition = new ColumnDefinition("name", "Varchar");

        String body = RestUtils.post(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(columnDefinition), HttpStatus.SC_CREATED);
        Map<String,String> response = objectMapper.readValue(body, Map.class);

        assertThat(response.get("name")).isEqualTo("name");

        body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true", host, keyspaceName, tableName, "name"),
                HttpStatus.SC_OK);
        ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
        assertThat(column).isEqualToComparingFieldByField(columnDefinition);
    }

    @Test
    public void addColumnPretty() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        ColumnDefinition columnDefinition = new ColumnDefinition("name", "Varchar");

        String body = RestUtils.post(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns?pretty=true", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(columnDefinition), HttpStatus.SC_CREATED);
        Map<String,String> response = objectMapper.readValue(body, Map.class);

        assertThat(response.get("name")).isEqualTo("name");

        body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true", host, keyspaceName, tableName, "name"),
                HttpStatus.SC_OK);
        ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
        assertThat(column).isEqualToComparingFieldByField(columnDefinition);
    }

    @Test
    public void addColumnBadType() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        ColumnDefinition columnDefinition = new ColumnDefinition("name", "badType");

        String body = RestUtils.post(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(columnDefinition), HttpStatus.SC_BAD_REQUEST);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void addColumnStatic() throws IOException {
        createKeyspace(keyspaceName);
        createTableWithClustering(keyspaceName, tableName);

        ColumnDefinition columnDefinition = new ColumnDefinition("balance", "Float", true);

        String body = RestUtils.post(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(columnDefinition), HttpStatus.SC_CREATED);
        Map<String,String> response = objectMapper.readValue(body, Map.class);

        assertThat(response.get("name")).isEqualTo("balance");

        body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true", host, keyspaceName, tableName, "balance"),
                HttpStatus.SC_OK);
        ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
        assertThat(column).isEqualToComparingFieldByField(columnDefinition);
    }

    @Test
    public void updateColumn() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        ColumnDefinition columnDefinition = new ColumnDefinition("identifier", "Uuid");

        String body = RestUtils.put(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, keyspaceName, tableName, "id"),
                objectMapper.writeValueAsString(columnDefinition), HttpStatus.SC_OK);
        Map<String,String> response = objectMapper.readValue(body, Map.class);

        assertThat(response.get("name")).isEqualTo("identifier");

        body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true", host, keyspaceName, tableName, "identifier"), HttpStatus.SC_OK);
        ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
        assertThat(column).isEqualToComparingFieldByField(columnDefinition);
    }

    @Test
    public void updateColumnPretty() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        ColumnDefinition columnDefinition = new ColumnDefinition("identifier", "Uuid");

        String body = RestUtils.put(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s?pretty=true", host, keyspaceName, tableName, "id"),
                objectMapper.writeValueAsString(columnDefinition), HttpStatus.SC_OK);
        Map<String,String> response = objectMapper.readValue(body, Map.class);

        assertThat(response.get("name")).isEqualTo("identifier");

        body = RestUtils.get(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s?raw=true", host, keyspaceName, tableName, "identifier"), HttpStatus.SC_OK);
        ColumnDefinition column = objectMapper.readValue(body, ColumnDefinition.class);
        assertThat(column).isEqualToComparingFieldByField(columnDefinition);
    }

    @Test
    public void updateColumnNotFound() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        ColumnDefinition columnDefinition = new ColumnDefinition("name", "text");

        String body = RestUtils.put(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, keyspaceName, tableName, "notFound"),
                objectMapper.writeValueAsString(columnDefinition), HttpStatus.SC_BAD_REQUEST);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void updateColumnBadTable() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        ColumnDefinition columnDefinition = new ColumnDefinition("name", "text");

        String body = RestUtils.put(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, keyspaceName, "foo", "age"),
                objectMapper.writeValueAsString(columnDefinition), HttpStatus.SC_BAD_REQUEST);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void updateColumnBadKeyspace() throws IOException {
        ColumnDefinition columnDefinition = new ColumnDefinition("name", "text");

        String body = RestUtils.put(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, "foo", tableName, "age"),
                objectMapper.writeValueAsString(columnDefinition), HttpStatus.SC_BAD_REQUEST);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void deleteColumn() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        RestUtils.delete(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, keyspaceName, tableName, "age"), HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void deleteColumnNotFound() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        RestUtils.delete(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, keyspaceName, tableName, "foo"),
                HttpStatus.SC_BAD_REQUEST);
    }

    @Test
    public void deleteColumnBadTable() throws IOException {
        createKeyspace(keyspaceName);

        String body = RestUtils.delete(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, keyspaceName, "foo", "age"), HttpStatus.SC_BAD_REQUEST);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void deleteColumnBadKeyspace() throws IOException {
        String body = RestUtils.delete(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, "foo", tableName, "age"), HttpStatus.SC_BAD_REQUEST);
        Error response = objectMapper.readValue(body, Error.class);

        assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
        assertThat(response.getDescription()).isNotEmpty();
    }

    @Test
    public void deleteColumnPartitionKey() throws IOException {
        createKeyspace(keyspaceName);
        createTable(keyspaceName, tableName);

        String body = RestUtils.delete(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables/%s/columns/%s", host, keyspaceName, tableName, "id"), HttpStatus.SC_BAD_REQUEST);
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

        RestUtils.post(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables", host, keyspaceName),
                objectMapper.writeValueAsString(tableAdd), HttpStatus.SC_CREATED);
    }

    private void createTableWithClustering(String keyspaceName, String tableName) throws IOException {
        TableAdd tableAdd = new TableAdd();
        tableAdd.setName(tableName);

        List<ColumnDefinition> columnDefinitions = new ArrayList<>();

        columnDefinitions.add(new ColumnDefinition("id", "uuid"));
        columnDefinitions.add(new ColumnDefinition("lastName", "text"));
        columnDefinitions.add(new ColumnDefinition("firstName", "text"));
        columnDefinitions.add(new ColumnDefinition("age", "int", true));
        columnDefinitions.add(new ColumnDefinition("expense_id", "int"));

        tableAdd.setColumnDefinitions(columnDefinitions);

        PrimaryKey primaryKey = new PrimaryKey();
        primaryKey.setPartitionKey(Collections.singletonList("id"));
        primaryKey.setClusteringKey(Collections.singletonList("expense_id"));
        tableAdd.setPrimaryKey(primaryKey);


        RestUtils.post(authToken, String.format("%s:8082/schemas/keyspaces/%s/tables", host, keyspaceName),
                objectMapper.writeValueAsString(tableAdd), HttpStatus.SC_CREATED);
    }

    private void createKeyspace(String keyspaceName) throws IOException {
        String createKeyspaceRequest = String.format("{\"name\": \"%s\", \"replicas\": 1}", keyspaceName);

        RestUtils.post(authToken, String.format("%s:8082/schemas/keyspaces", host),
                createKeyspaceRequest, HttpStatus.SC_CREATED);
    }

    private String setupClusteringTestCase() throws IOException {
        createKeyspace(keyspaceName);
        createTableWithClustering(keyspaceName, tableName);

        String rowIdentifier = UUID.randomUUID().toString();
        Map<String, String> row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");
        row.put("expense_id", "1");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        row = new HashMap<>();
        row.put("id", rowIdentifier);
        row.put("firstName", "John");
        row.put("expense_id", "2");

        RestUtils.post(authToken, String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
                objectMapper.writeValueAsString(row), HttpStatus.SC_CREATED);

        return rowIdentifier;
    }
}
