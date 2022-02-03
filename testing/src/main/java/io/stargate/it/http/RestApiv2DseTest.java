package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.web.restapi.models.GetResponseWrapper;
import io.stargate.web.restapi.models.IndexAdd;
import io.stargate.web.restapi.models.IndexKind;
import io.stargate.web.restapi.models.SuccessResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;

// JUnit 5 requires both annotations, see
// https://stackoverflow.com/questions/63250350/junit-5-enabledifsystemproperty-doesnt-work-as-expected
@EnabledIfSystemProperty(named = "ccm.dse", matches = "true")
@DisabledIfSystemProperty(named = "ccm.dse", matches = "(?!true)")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      // List
      "CREATE TABLE lists(k int PRIMARY KEY, l list<int>)",
      "CREATE CUSTOM INDEX lists_l_idx ON lists(l) USING 'StorageAttachedIndex'",
      "INSERT INTO lists (k,l) VALUES (1, [1,2,3])",
      // Set
      "CREATE TABLE sets(k int PRIMARY KEY, s set<int>)",
      "CREATE CUSTOM INDEX sets_s_idx ON sets(s) USING 'StorageAttachedIndex'",
      "INSERT INTO sets (k,s) VALUES (1, {1,2,3})",
      // Map, indexed by key
      "CREATE TABLE maps_per_key(k int PRIMARY KEY, m map<int, text>)",
      "CREATE CUSTOM INDEX maps_per_key_m_idx ON maps_per_key(keys(m)) USING 'StorageAttachedIndex'",
      "INSERT INTO maps_per_key (k,m) values (1, {1:'a',2:'b',3:'c'})",
      // Map, indexed by value
      "CREATE TABLE maps_per_value(k int PRIMARY KEY, m map<int, text>)",
      "CREATE CUSTOM INDEX maps_per_value_m_idx ON maps_per_value(m) USING 'StorageAttachedIndex'",
      "INSERT INTO maps_per_value (k,m) values (1, {1:'a',2:'b',3:'c'})",
      // Map, indexed by entry
      "CREATE TABLE maps_per_entry(k int PRIMARY KEY, m map<int, text>)",
      "CREATE CUSTOM INDEX maps_per_entry_m_idx ON maps_per_entry(entries(m)) USING 'StorageAttachedIndex'",
      "INSERT INTO maps_per_entry (k,m) values (1, {1:'a',2:'b',3:'c'})",
      // Table for index test
      "CREATE TABLE index_test_table(k int PRIMARY KEY, l list<int>, m1 map<int, text>, m2 map<int, text>, m3 map<int, text>)",
    })
@ExtendWith(ApiServiceExtension.class)
@ApiServiceSpec(parametersCustomizer = "buildApiServiceParameters")
public class RestApiv2DseTest extends BaseRestApiTest {

  private static final ObjectMapper objectMapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static String keyspaceUri;
  private static String schemaUri;
  private static String authToken;

  @BeforeAll
  public static void beforeAll(
      StargateConnectionInfo cluster,
      @TestKeyspace CqlIdentifier keyspaceId,
      ApiServiceConnectionInfo restApi)
      throws IOException {
    String restUrlBase = restApi.host() + ":" + restApi.port();
    authToken = fetchAuthToken(cluster.seedAddress());
    keyspaceUri = String.format("http://%s/v2/keyspaces/%s", restUrlBase, keyspaceId.asInternal());
    schemaUri =
        String.format("http://%s/v2/schemas/keyspaces/%s", restUrlBase, keyspaceId.asInternal());
  }

  @Test
  @DisplayName("Should query list column with $contains")
  public void listContainsTest() throws IOException {
    List<Map<String, Object>> data = query("/lists?where={\"l\":{\"$contains\":1}}");
    assertThat(data).hasSize(1);
    assertThat(data.get(0).get("k")).isEqualTo(1);

    data = query("/lists?where={\"l\":{\"$contains\":4}}");
    assertThat(data).hasSize(0);
  }

  @Test
  @DisplayName("Should query set column with $contains")
  public void setContainsTest() throws IOException {
    List<Map<String, Object>> data = query("/sets?where={\"s\":{\"$contains\":1}}");
    assertThat(data).hasSize(1);
    assertThat(data.get(0).get("k")).isEqualTo(1);

    data = query("/sets?where={\"s\":{\"$contains\":4}}");
    assertThat(data).hasSize(0);
  }

  @Test
  @DisplayName("Should query map column with $containsKey")
  public void mapContainsKeyTest() throws IOException {
    List<Map<String, Object>> data = query("/maps_per_key?where={\"m\":{\"$containsKey\":1}}");
    assertThat(data).hasSize(1);
    assertThat(data.get(0).get("k")).isEqualTo(1);

    data = query("/maps_per_key?where={\"m\":{\"$containsKey\":4}}");
    assertThat(data).hasSize(0);
  }

  @Test
  @DisplayName("Should query map column with $contains")
  public void mapContainsTest() throws IOException {
    List<Map<String, Object>> data = query("/maps_per_value?where={\"m\":{\"$contains\":\"a\"}}");
    assertThat(data).hasSize(1);
    assertThat(data.get(0).get("k")).isEqualTo(1);

    data = query("/maps_per_value?where={\"m\":{\"$contains\":\"d\"}}");
    assertThat(data).hasSize(0);
  }

  @Test
  @DisplayName("Should query map column with $containsEntry")
  public void mapContainsEntryTest() throws IOException {
    List<Map<String, Object>> data =
        query("/maps_per_entry?where={\"m\":{\"$containsEntry\":{\"key\": 1, \"value\": \"a\"}}}");
    assertThat(data).hasSize(1);
    assertThat(data.get(0).get("k")).isEqualTo(1);

    data =
        query("/maps_per_entry?where={\"m\":{\"$containsEntry\":{\"key\": 1, \"value\": \"b\"}}}");
    assertThat(data).hasSize(0);
  }

  @Test
  public void createCustomIndexes() throws IOException {
    IndexAdd indexAdd = new IndexAdd();
    indexAdd.setName("idx1");
    indexAdd.setColumn("l");
    indexAdd.setIfNotExists(false);
    indexAdd.setType("StorageAttachedIndex");
    createIndex("index_test_table", indexAdd);

    indexAdd.setName("idx2");
    indexAdd.setColumn("m1");
    indexAdd.setKind(IndexKind.KEYS);
    createIndex("index_test_table", indexAdd);

    indexAdd.setName("idx3");
    indexAdd.setColumn("m2");
    indexAdd.setKind(null);
    createIndex("index_test_table", indexAdd);

    indexAdd.setName("idx4");
    indexAdd.setColumn("m3");
    indexAdd.setKind(IndexKind.ENTRIES);
    createIndex("index_test_table", indexAdd);
  }

  private void createIndex(String tableName, IndexAdd indexAdd) throws IOException {
    String body =
        RestUtils.post(
            authToken,
            String.format("%s/tables/%s/indexes", schemaUri, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_CREATED);
    SuccessResponse successResponse =
        objectMapper.readValue(body, new TypeReference<SuccessResponse>() {});
    assertThat(successResponse.getSuccess()).isTrue();
  }

  private static String fetchAuthToken(String host) throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format(
                "http://%s:8081/v1/auth/token/generate", host), // TODO: make auth port configurable
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    String token = authTokenResponse.getAuthToken();
    assertThat(token).isNotNull();
    return token;
  }

  private List<Map<String, Object>> query(String uri) throws IOException {
    String body = RestUtils.get(authToken, keyspaceUri + uri, HttpStatus.SC_OK);
    GetResponseWrapper<?> getResponseWrapper =
        objectMapper.readValue(body, GetResponseWrapper.class);
    return objectMapper.convertValue(
        getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
  }
}
