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
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.web.models.ApiError;
import io.stargate.web.restapi.models.ColumnDefinition;
import io.stargate.web.restapi.models.GetResponseWrapper;
import io.stargate.web.restapi.models.IndexAdd;
import io.stargate.web.restapi.models.IndexKind;
import io.stargate.web.restapi.models.PrimaryKey;
import io.stargate.web.restapi.models.RESTResponseWrapper;
import io.stargate.web.restapi.models.SuccessResponse;
import io.stargate.web.restapi.models.TableAdd;
import io.stargate.web.restapi.models.TableResponse;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
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
 * Integration tests for REST API v2 that cover Row access CRUD methods.
 *
 * <p>Due to historical reasons, some Scheme CRUD tests are also included until moved out to new,
 * more granular IT classes.
 */
@NotThreadSafe
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec()
public class RestApiv2RowsTest extends BaseIntegrationTest {
  private String keyspaceName;
  private String tableName;
  private static String authToken;
  private String host;
  private String restUrlBase;

  // NOTE! Does not automatically disable exception on unknown properties to have
  // stricter matching of expected return types: if needed, can override on
  // per-ObjectReader basis
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final ObjectReader LIST_OF_MAPS_GETRESPONSE_READER =
      objectMapper.readerFor(ListOfMapsGetResponseWrapper.class);

  static class ListOfMapsGetResponseWrapper extends GetResponseWrapper<List<Map<String, Object>>> {
    public ListOfMapsGetResponseWrapper() {
      super(-1, null, null);
    }
  }

  static class JsonNodeGetResponseWrapper extends GetResponseWrapper<JsonNode> {
    public JsonNodeGetResponseWrapper() {
      super(-1, null, null);
    }
  }

  // TablesResource specifies only as "Map" but it looks to me like:
  static class NameResponse {
    public String name;
  }

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
  /* Test methods for Index CRUD operations
  /************************************************************************
   */

  /*
  /************************************************************************
  /* Test methods for Row Access CRUD operations
  /************************************************************************
   */

  @Test
  public void getRowsWithQuery() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);
    JsonNodeGetResponseWrapper getResponseWrapper =
        objectMapper.readValue(body, JsonNodeGetResponseWrapper.class);
    JsonNode data = getResponseWrapper.getData();
    assertThat(data.size()).isEqualTo(1);
    assertThat(data.at("/0/id").asText()).isEqualTo(rowIdentifier);
    assertThat(data.at("/0/firstName").asText()).isEqualTo("John");
  }

  @Test
  public void getRowsWithQuery2Filters() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "age int", "firstName text"),
        Collections.singletonList("id"),
        Arrays.asList("age", "firstName"));

    insertTestTableRows(
        Arrays.asList(
            Arrays.asList("id 1", "firstName Bob", "age 25"),
            Arrays.asList("id 1", "firstName Dave", "age 40"),
            Arrays.asList("id 1", "firstName Fred", "age 63")));

    // Test the case where we have 2 filters ($gt and $lt) for one field
    String whereClause = "{\"id\":{\"$eq\":\"1\"},\"age\":{\"$gt\":30,\"$lt\":50}}";
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);
    JsonNodeGetResponseWrapper getResponseWrapper =
        objectMapper.readValue(body, JsonNodeGetResponseWrapper.class);
    JsonNode data = getResponseWrapper.getData();
    assertThat(data.size()).isEqualTo(1);
    assertThat(data.at("/0/id").asText()).isEqualTo("1");
    assertThat(data.at("/0/firstName").asText()).isEqualTo("Dave");
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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(getResponseWrapper.getPageState()).isNotEmpty();
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
    assertThat(data.get(0).get("expense_id")).isEqualTo(1);
  }

  @Test
  public void getRowsWithQueryAndRaw() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
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
  public void getRowsWithQueryAndInvalidSort() throws IOException {
    String rowIdentifier = setupClusteringTestCase();

    String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", rowIdentifier);
    RestUtils.get(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s?where=%s&sort={\"expense_id\"\":\"desc\"}",
            host, keyspaceName, tableName, whereClause),
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void getRowsWithNotFound() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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
                "%s:8082/v2/keyspaces/%s/%s?where=%s&fields=id,firstName",
                host, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
    assertThat(getResponseWrapper.getCount()).isEqualTo(0);
    assertThat(data).isEmpty();
  }

  @Test
  public void getRowsWithInQuery() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text"),
        Collections.singletonList("id"),
        Collections.singletonList("firstName"));

    insertTestTableRows(
        Arrays.asList(
            Arrays.asList("id 1", "firstName John"),
            Arrays.asList("id 1", "firstName Sarah"),
            Arrays.asList("id 2", "firstName Jane")));

    String whereClause = "{\"id\":{\"$eq\":\"1\"},\"firstName\":{\"$in\":[\"Sarah\"]}}";
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.size()).isEqualTo(1);
    assertThat(data.get(0).get("id")).isEqualTo("1");
    assertThat(data.get(0).get("firstName")).isEqualTo("Sarah");

    // Let's also test with three values (of which 2 match)
    whereClause =
        "{\"id\":{\"$eq\":\"1\"},\"firstName\":{\"$in\":[\"Sarah\", \"Bob\", \"John\" ]}}";
    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    JsonNode root = objectMapper.readTree(body);
    Set<String> namesReceived =
        new LinkedHashSet<>(
            Arrays.asList(
                root.path(0).path("firstName").asText(), root.path(1).path("firstName").asText()));
    Set<String> namesExpected = new LinkedHashSet<>(Arrays.asList("Sarah", "John"));
    assertThat(namesReceived).isEqualTo(namesExpected);
  }

  // 04-Jan-2022, tatu: Verifies existing behavior of Stargate REST 1.0,
  //   which seems to differ from Documents API. Whether right or wrong,
  //   this behavior is what exists.
  @Test
  public void getRowsWithExistsQuery() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text", "enabled boolean"),
        Collections.singletonList("id"),
        Collections.singletonList("enabled"));

    insertTestTableRows(
        Arrays.asList(
            Arrays.asList("id 1", "firstName Bob", "enabled false"),
            Arrays.asList("id 1", "firstName Dave", "enabled true"),
            Arrays.asList("id 2", "firstName Frank", "enabled true"),
            Arrays.asList("id 1", "firstName Pete", "enabled false")));

    String whereClause = "{\"id\":{\"$eq\":\"1\"},\"enabled\":{\"$exists\":true}}";
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);
    JsonNode json = objectMapper.readTree(body);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.at("/0/firstName").asText()).isEqualTo("Dave");
  }

  @Test
  public void getRowsWithSetContainsQuery() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "tags set<text>", "firstName text"),
        Collections.singletonList("id"),
        Collections.singletonList("firstName"));
    // Cannot query against non-key columns, unless there's an index, so:
    createTestIndex(keyspaceName, tableName, "tags", "tags_index", false, IndexKind.VALUES);

    insertTestTableRows(
        Arrays.asList(
            Arrays.asList("id 1", "firstName Bob", "tags {'a','b'}"),
            Arrays.asList("id 1", "firstName Dave", "tags {'b','c'}"),
            Arrays.asList("id 1", "firstName Fred", "tags {'x'}")));

    // First, no match
    String whereClause = "{\"id\":{\"$eq\":\"1\"},\"tags\":{\"$contains\":\"z\"}}";
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);
    JsonNode json = objectMapper.readTree(body);
    assertThat(json.size()).isEqualTo(0);

    // and then 2 matches
    whereClause = "{\"id\":{\"$eq\":\"1\"},\"tags\": {\"$contains\": \"b\"}}";
    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);
    json = objectMapper.readTree(body);
    assertThat(json.size()).isEqualTo(2);
    assertThat(json.at("/0/firstName").asText()).isEqualTo("Bob");
    assertThat(json.at("/1/firstName").asText()).isEqualTo("Dave");

    // 05-Jan-2022, tatu: API does allow specifying an ARRAY of things to contain, but,
    //    alas, resulting query will not work ("need to ALLOW FILTERING").
    //    So not testing that case.
  }

  @Test
  public void getRowsWithContainsKeyQuery() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "attributes map<text,text>", "firstName text"),
        Collections.singletonList("id"),
        Collections.singletonList("firstName"));
    // Cannot query against non-key columns, unless there's an index, so:
    createTestIndex(
        keyspaceName, tableName, "attributes", "attributes_map_index", false, IndexKind.KEYS);

    insertTestTableRows(
        Arrays.asList(
            Arrays.asList("id 1", "firstName Bob", "attributes {'a':'1'}"),
            Arrays.asList("id 1", "firstName Dave", "attributes {'b':'2'}"),
            Arrays.asList("id 1", "firstName Fred", "attributes {'c':'3'}")));

    // First, no match
    String whereClause = "{\"id\":{\"$eq\":\"1\"},\"attributes\":{\"$containsKey\":\"d\"}}";
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);
    JsonNode json = objectMapper.readTree(body);
    assertThat(json.size()).isEqualTo(0);

    // and then a single match
    whereClause = "{\"id\":{\"$eq\":\"1\"},\"attributes\":{\"$containsKey\":\"b\"}}";
    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);
    json = objectMapper.readTree(body);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.at("/0/firstName").asText()).isEqualTo("Dave");

    // 06-Jan-2022, tatu: API does allow specifying an ARRAY of things to contain, but,
    //    alas, resulting query will not work ("need to ALLOW FILTERING").
    //    So not testing that case.
  }

  @Test
  public void getRowsWithContainsEntryQuery() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "attributes map<text,text>", "firstName text"),
        Collections.singletonList("id"),
        Collections.singletonList("firstName"));
    // Cannot query against non-key columns, unless there's an index, so:
    createTestIndex(
        keyspaceName, tableName, "attributes", "attributes_map_index", false, IndexKind.ENTRIES);

    insertTestTableRows(
        Arrays.asList(
            Arrays.asList("id 1", "firstName Bob", "attributes {'a':'1'}"),
            Arrays.asList("id 1", "firstName Dave", "attributes {'b':'2'}"),
            Arrays.asList("id 1", "firstName Fred", "attributes {'c':'3'}")));

    // First, no match
    String whereClause =
        "{\"id\":{\"$eq\":\"1\"},\"attributes\":{\"$containsEntry\":{\"key\":\"b\",\"value\":\"1\"}}}";
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);
    JsonNode json = objectMapper.readTree(body);
    assertThat(json.size()).isEqualTo(0);

    // and then a single match
    whereClause =
        "{\"id\":{\"$eq\":\"1\"},\"attributes\":{\"$containsEntry\":{\"key\":\"c\",\"value\":\"3\"}}}";
    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);
    json = objectMapper.readTree(body);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.at("/0/firstName").asText()).isEqualTo("Fred");
  }

  @Test
  public void getRowsWithTimestampQuery() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text", "created timestamp"),
        Collections.singletonList("id"),
        Collections.singletonList("created"));

    String timestamp = "2021-04-23T18:42:22.139Z";
    insertTestTableRows(
        Arrays.asList(
            Arrays.asList("id 1", "firstName John", "created " + timestamp),
            Arrays.asList("id 1", "firstName Sarah", "created 2021-04-20T18:42:22.139Z"),
            Arrays.asList("id 2", "firstName Jane", "created 2021-04-22T18:42:22.139Z")));

    String whereClause =
        String.format("{\"id\":{\"$eq\":\"1\"},\"created\":{\"$in\":[\"%s\"]}}", timestamp);
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s?where=%s&raw=true",
                host, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.size()).isEqualTo(1);
    assertThat(data.get(0).get("id")).isEqualTo("1");
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
    assertThat(data.get(0).get("created")).isEqualTo(timestamp);
  }

  @Test
  public void getRowsWithDurationValue() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text", "time duration"),
        Collections.singletonList("id"),
        Collections.singletonList("firstName"));

    CqlDuration expDuration = CqlDuration.from("2w");
    insertTestTableRows(
        Arrays.asList(
            Arrays.asList("id 1", "firstName John", "time 2d"),
            Arrays.asList("id 2", "firstName Sarah", "time " + expDuration),
            Arrays.asList("id 3", "firstName Jane", "time 30h20m")));

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s/%s?raw=true", restUrlBase, keyspaceName, tableName, "2"),
            HttpStatus.SC_OK);
    JsonNode json = objectMapper.readTree(body);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.at("/0/firstName").asText()).isEqualTo("Sarah");
    // NOTE: "2 weeks" may become "14 days" (or vice versa); so let's compare CqlDuration equality
    assertThat(CqlDuration.from(json.at("/0/time").asText())).isEqualTo(expDuration);
  }

  @Test
  public void getAllRowsWithPaging() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text"),
        Collections.singletonList("id"),
        null);

    List<Map<String, String>> expRows =
        insertTestTableRows(
            Arrays.asList(
                Arrays.asList("id 1", "firstName Jonh"),
                Arrays.asList("id 2", "firstName Jane"),
                Arrays.asList("id 3", "firstName Scott"),
                Arrays.asList("id 4", "firstName April")));
    final List<Map<String, Object>> allRows = new ArrayList<>();

    // get first page
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/rows?page-size=2", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    assertThat(getResponseWrapper.getPageState()).isNotEmpty();
    allRows.addAll(getResponseWrapper.getData());

    // get second page
    String pageState = getResponseWrapper.getPageState();
    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/rows?page-size=2&page-state=%s",
                host, keyspaceName, tableName, pageState),
            HttpStatus.SC_OK);

    getResponseWrapper = LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    allRows.addAll(getResponseWrapper.getData());

    // ensure no more pages: we do still get PagingState, but no more rows
    pageState = getResponseWrapper.getPageState();
    assertThat(pageState).isNotEmpty();
    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/rows?page-size=2&page-state=%s",
                host, keyspaceName, tableName, pageState),
            HttpStatus.SC_OK);
    getResponseWrapper = LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    assertThat(getResponseWrapper.getCount()).isEqualTo(0);
    assertThat(getResponseWrapper.getPageState()).isNull();

    // Since order in which we get these is arbitrary (wrt partition key), need
    // to go from List to Set
    assertThat(new LinkedHashSet(allRows)).isEqualTo(new LinkedHashSet(expRows));
  }

  @Test
  public void getAllRowsNoPaging() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text"),
        Collections.singletonList("id"),
        null);

    List<Map<String, String>> expRows =
        insertTestTableRows(
            Arrays.asList(
                Arrays.asList("id 1", "firstName Jonh"),
                Arrays.asList("id 2", "firstName Jane"),
                Arrays.asList("id 3", "firstName Scott"),
                Arrays.asList("id 4", "firstName April")));

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/rows?fields=id, firstName",
                host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    @SuppressWarnings("rawtypes")
    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    assertThat(getResponseWrapper.getCount()).isEqualTo(4);

    // Alas, due to "id" as partition key, ordering is arbitrary; so need to
    // convert from List to something like Set
    List<Map<String, Object>> rows = getResponseWrapper.getData();

    assertThat(rows.size()).isEqualTo(4);
    assertThat(new LinkedHashSet<>(rows)).isEqualTo(new LinkedHashSet<>(expRows));
  }

  @Test
  public void getInvalidWhereClause() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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

    ApiError response = objectMapper.readValue(body, ApiError.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription())
        .isEqualTo("Bad request: Unknown field name 'invalid_field'.");
  }

  @Test
  public void getRows() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

    // To try to ensure we actually find the right entry, create one other entry first
    Map<String, String> row = new HashMap<>();
    row.put("id", UUID.randomUUID().toString());
    row.put("firstName", "Michael");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    // and then the row we are actually looking for:
    String rowIdentifier = UUID.randomUUID().toString();
    row = new HashMap<>();
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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
    // Verify we fetch one and only one entry
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(data.size()).isEqualTo(1);
    // and that its contents match
    assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
  }

  // Test for inserting and fetching row(s) with Tuple values: inserts using
  // "Stringified" (non-JSON, CQL literal) notation.
  @Test
  public void getRowsWithTupleStringified() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "data tuple<int,boolean,text>", "alt_id uuid"),
        Collections.singletonList("id"),
        Collections.emptyList());
    String altUid1 = UUID.randomUUID().toString();
    insertTestTableRows(
        Arrays.asList(
            // Put UUID for the first row; leave second one empty/missing
            Arrays.asList("id 1", "data (28,false,'foobar')", "alt_id " + altUid1),
            Arrays.asList("id 2", "data (39,true,'bingo')")));
    JsonNode json = readRawRowsBySingleKey(keyspaceName, tableName, "2");
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.at("/0/id").asText()).isEqualTo("2");
    assertThat(json.at("/0/data/0").intValue()).isEqualTo(39);
    assertThat(json.at("/0/data/1").booleanValue()).isTrue();
    assertThat(json.at("/0/data").size()).isEqualTo(3);
    assertTrue(json.at("/0/alt_id").isNull());
  }

  // Test for inserting and fetching row(s) with Tuple values: inserts using
  // standard JSON payload
  @Test
  public void getRowsWithTupleTyped() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(
        tableName,
        Arrays.asList("id text", "data tuple<int,boolean,text>", "alt_id uuid"),
        Collections.singletonList("id"),
        Collections.emptyList());
    String altUid1 = UUID.randomUUID().toString();
    insertTypedTestTableRows(
        Arrays.asList(
            ImmutableMap.of(
                "id", "1", "data", Arrays.asList(28, false, "foobar"), "alt_id", altUid1),
            ImmutableMap.of(
                "id",
                "2",
                "data",
                Arrays.asList(39, true, "bingo"),
                "alt_id",
                objectMapper.nullNode())));
    JsonNode json = readRawRowsBySingleKey(keyspaceName, tableName, "2");
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.at("/0/id").asText()).isEqualTo("2");
    assertThat(json.at("/0/data/0").intValue()).isEqualTo(39);
    assertThat(json.at("/0/data/1").booleanValue()).isTrue();
    assertThat(json.at("/0/data").size()).isEqualTo(3);
    assertTrue(json.at("/0/alt_id").isNull());
  }

  @Test
  public void getRowsWithUDT() throws IOException {
    createTestKeyspace(keyspaceName);

    // create UDT: note -- UDT names must be lower-case it seems (mixed case fails)
    String udtString =
        "{\"name\": \"testUDT\", \"fields\":"
            + "[{\"name\":\"name\",\"typeDefinition\":\"text\"},"
            + "{\"name\":\"age\",\"typeDefinition\":\"int\"}]}";
    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/types", restUrlBase, keyspaceName),
        udtString,
        HttpStatus.SC_CREATED);

    createTestTable(
        tableName,
        Arrays.asList("id text", "details testUDT"),
        Collections.singletonList("id"),
        Collections.emptyList());

    insertTestTableRows(
        Arrays.asList(
            Arrays.asList("id 1", "details {name:'Bob',age:36}"),
            Arrays.asList("id 2", "details {name:'Alice',age:29}"),
            Arrays.asList("id 3", "details {name:'Peter',age:75}")));

    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s/%s?raw=true", restUrlBase, keyspaceName, tableName, "2"),
            HttpStatus.SC_OK);
    JsonNode json = objectMapper.readTree(body);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.at("/0/details/name").asText()).isEqualTo("Alice");
    assertThat(json.at("/0/details/age").intValue()).isEqualTo(29);
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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(getResponseWrapper.getPageState()).isNotEmpty();
    assertThat(data.get(0).get("id")).isEqualTo(1);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
    assertThat(data.get(0).get("expense_id")).isEqualTo(1);
  }

  @Test
  public void getRowsNotFound() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
    assertThat(getResponseWrapper.getCount()).isEqualTo(0);
    assertThat(data).isEmpty();
  }

  @Test
  public void getRowsRaw() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
    assertThat(getResponseWrapper.getCount()).isEqualTo(2);
    assertThat(data.get(0).get("v")).isEqualTo(9);
    assertThat(data.get(1).get("v")).isEqualTo(19);

    body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1/20", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    getResponseWrapper = LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    data = getResponseWrapper.getData();
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(data.get(0).get("v")).isEqualTo(19);
  }

  @Test
  public void addRow() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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
    createTestKeyspace(keyspaceName);
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
    createTestKeyspace(keyspaceName);
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
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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

    ApiError response = objectMapper.readValue(body, ApiError.class);

    assertThat(response.getCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(response.getDescription())
        .isEqualTo("Bad request: Unknown field name 'invalid_field'.");
  }

  @Test
  public void addRowWithInvalidJson() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        "{\"id\": \"af2603d2-8c03-11eb-a03f-0ada685e0000\",\"firstName: \"john\"}",
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void updateRow() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("firstName", "Robert");
    rowUpdate.put("lastName", "Plant");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s/%s", restUrlBase, keyspaceName, tableName, rowIdentifier),
            objectMapper.writeValueAsString(rowUpdate),
            HttpStatus.SC_OK);
    Map<String, String> data = readWrappedRESTResponse(body, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);
  }

  @Test
  public void updateRowRaw() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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

    // Also verify that we can "delete" lastName
    Map<String, String> update2 = new HashMap<>();
    update2.put("firstName", "Roger");
    update2.put("lastName", null);
    RestUtils.put(
        authToken,
        String.format(
            "%s/v2/keyspaces/%s/%s/%s", restUrlBase, keyspaceName, tableName, rowIdentifier),
        objectMapper.writeValueAsString(update2),
        HttpStatus.SC_OK);

    // And that change actually occurs
    JsonNode json = readRawRowsBySingleKey(keyspaceName, tableName, rowIdentifier);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.at("/0/id").asText()).isEqualTo(rowIdentifier);
    assertThat(json.at("/0/firstName").asText()).isEqualTo("Roger");
    assertTrue(json.at("/0/lastName").isNull());
    assertTrue(json.at("/0/age").isNull());
    assertThat(json.at("/0").size()).isEqualTo(4);
  }

  @Test
  public void updateRowWithCounter() throws IOException {
    createTestKeyspace(keyspaceName);
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
    createTestKeyspace(keyspaceName);
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
  public void updateRowWithInvalidJson() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    RestUtils.put(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
        "{\"firstName\": \"Robert,\"lastName\": \"Plant\"}",
        HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void patchRow() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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
    Map<String, String> patchData = readWrappedRESTResponse(body, Map.class);
    assertThat(patchData).containsAllEntriesOf(rowUpdate);

    body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
            HttpStatus.SC_OK);

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
    assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("firstName")).isEqualTo("Jane");
    assertThat(data.get(0).get("lastName")).isEqualTo("Doe");
  }

  @Test
  public void patchRowRaw() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
    assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("firstName")).isEqualTo("Jane");
    assertThat(data.get(0).get("lastName")).isEqualTo("Doe");
  }

  @Test
  public void deleteRow() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
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

    getResponseWrapper = LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    data = getResponseWrapper.getData();
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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
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

    getResponseWrapper = LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    assertThat(getResponseWrapper.getCount()).isEqualTo(0);

    body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, "2"),
            HttpStatus.SC_OK);

    getResponseWrapper = LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    data = getResponseWrapper.getData();
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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
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

    getResponseWrapper = LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
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

    ListOfMapsGetResponseWrapper getResponseWrapper =
        LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    List<Map<String, Object>> data = getResponseWrapper.getData();
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

    getResponseWrapper = LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    assertThat(getResponseWrapper.getCount()).isEqualTo(0);

    body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/1/one/-1", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    getResponseWrapper = LIST_OF_MAPS_GETRESPONSE_READER.readValue(body);
    assertThat(getResponseWrapper.getCount()).isEqualTo(1);
    assertThat(data.get(0).get("v")).isEqualTo(9);
  }

  @Test
  public void deleteRowNoSuchKey() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTable(keyspaceName, tableName);

    // First, try deleting row with valid UUID but one for which there is no row
    // We should just get usual "NO_CONTENT" as DELETE is idempotent

    final String rowIdentifier = UUID.randomUUID().toString();
    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
        HttpStatus.SC_NO_CONTENT);

    // But then see what happens with invalid key (String that is not UUID, in this case)
    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, "not-really-an-uuid"),
        HttpStatus.SC_BAD_REQUEST);
  }

  /*
  /************************************************************************
  /* Helper methods for setting up tests
  /************************************************************************
   */

  private void createTestTable(String keyspaceName, String tableName) throws IOException {
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
        String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
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
            String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
            objectMapper.writeValueAsString(tableAdd),
            HttpStatus.SC_CREATED);

    TableResponse tableResponse = objectMapper.readValue(body, TableResponse.class);
    assertThat(tableResponse.getName()).isEqualTo(tableName);
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

  private void createTestTableWithMixedClustering(String keyspaceName, String tableName)
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
        String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_CREATED);
  }

  private void createTestKeyspace(String keyspaceName) throws IOException {
    String createKeyspaceRequest =
        String.format("{\"name\": \"%s\", \"replicas\": 1}", keyspaceName);

    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces", restUrlBase),
        createKeyspaceRequest,
        HttpStatus.SC_CREATED);
  }

  private void createTestIndex(
      String keyspaceName,
      String tableName,
      String columnName,
      String indexName,
      boolean ifNotExists,
      IndexKind kind)
      throws IOException {
    IndexAdd indexAdd = new IndexAdd();
    indexAdd.setColumn(columnName);
    indexAdd.setName(indexName);
    indexAdd.setIfNotExists(ifNotExists);
    indexAdd.setKind(kind);

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
  }

  /**
   * Helper method for inserting table entries using so-called "Stringified" values for columns:
   * this differs a bit from full JSON values and is mostly useful for simple String and number
   * fields.
   *
   * @return {@code List} of entries to expect back for given definitions.
   */
  private List<Map<String, String>> insertTestTableRows(List<List<String>> rows)
      throws IOException {
    final List<Map<String, String>> insertedRows = new ArrayList<>();
    for (List<String> row : rows) {
      Map<String, String> rowMap = new HashMap<>();
      for (String kv : row) {
        // Split on first space, leave others in (with no limit we'd silently
        // drop later space-separated parts)
        String[] parts = kv.split(" ", 2);
        rowMap.put(parts[0].trim(), parts[1].trim());
      }
      insertedRows.add(rowMap);

      RestUtils.post(
          authToken,
          String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
          objectMapper.writeValueAsString(rowMap),
          HttpStatus.SC_CREATED);
    }
    return insertedRows;
  }

  // Variant of "insertTestTableRows" in which entries are defined as partially
  // typed Java Objects and serialized as real JSON and NOT as "Stringified"
  // CQL-style format.
  private void insertTypedTestTableRows(List<Map<String, Object>> rows) throws IOException {
    for (Map<String, Object> row : rows) {
      RestUtils.post(
          authToken,
          String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
          objectMapper.writeValueAsString(row),
          HttpStatus.SC_CREATED);
    }
  }

  private String setupClusteringTestCase() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTableWithClustering(keyspaceName, tableName);

    String rowIdentifier = "1";
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");
    row.put("expense_id", "1");

    RestUtils.post(
        authToken,
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");
    row.put("expense_id", "2");

    RestUtils.post(
        authToken,
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    row = new HashMap<>();
    row.put("id", "2");
    row.put("firstName", "Jane");
    row.put("expense_id", "1");

    RestUtils.post(
        authToken,
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    row = new HashMap<>();
    row.put("id", "2");
    row.put("firstName", "Jane");
    row.put("expense_id", "1");

    RestUtils.post(
        authToken,
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);

    return rowIdentifier;
  }

  private void setupMixedClusteringTestCase() throws IOException {
    createTestKeyspace(keyspaceName);
    createTestTableWithMixedClustering(keyspaceName, tableName);

    Map<String, String> row = new HashMap<>();
    row.put("pk0", "1");
    row.put("pk1", "one");
    row.put("pk2", "-1");
    row.put("ck0", "10");
    row.put("ck1", "foo");
    row.put("v", "9");

    RestUtils.post(
        authToken,
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
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
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
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
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);
  }

  private <T> T readWrappedRESTResponse(String body, Class<T> wrappedType) throws IOException {
    JavaType wrapperType =
        objectMapper
            .getTypeFactory()
            .constructParametricType(RESTResponseWrapper.class, wrappedType);
    RESTResponseWrapper<T> wrapped = objectMapper.readValue(body, wrapperType);
    return wrapped.getData();
  }

  // Simple helper method for the case of single primary key, standard auth token;
  // will use "raw" Rows endpoint to access entries, return as Tree (JsonNode)
  private JsonNode readRawRowsBySingleKey(String keyspaceName, String tableName, Object rowId)
      throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s/%s?raw=true", restUrlBase, keyspaceName, tableName, rowId),
            HttpStatus.SC_OK);
    return objectMapper.readTree(body);
  }
}
