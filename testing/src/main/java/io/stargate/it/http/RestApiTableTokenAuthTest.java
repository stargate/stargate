package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import io.stargate.web.models.ColumnDefinition;
import io.stargate.web.models.GetResponseWrapper;
import io.stargate.web.models.PrimaryKey;
import io.stargate.web.models.TableAdd;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE ROLE IF NOT EXISTS 'read_only_user' WITH PASSWORD = 'read_only_user' AND LOGIN = TRUE",
      "CREATE KEYSPACE IF NOT EXISTS table_token_test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}",
      "CREATE TABLE IF NOT EXISTS table_token_test.tbl_test (key text PRIMARY KEY, value text);",
      "INSERT INTO table_token_test.tbl_test (key, value) VALUES ('a', 'alpha')",
      "GRANT SELECT ON KEYSPACE table_token_test TO read_only_user",
    })
public class RestApiTableTokenAuthTest extends BaseOsgiIntegrationTest {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final String keyspaceName = "table_token_test";
  private final String tableName = "tbl_test";
  private final String readOnlyUsername = "read_only_user";
  private final String readOnlyPassword = "read_only_user";

  private String host;

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.enableAuth(true);
    builder.putSystemProperties("stargate.auth_id", "AuthTableBasedService");
  }

  @BeforeEach
  public void setup(StargateConnectionInfo cluster) {
    host = "http://" + cluster.seedAddress();

    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Test
  public void getRowsV2() throws IOException {
    String authToken = generateToken(readOnlyUsername, readOnlyPassword);

    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, "a"),
            HttpStatus.SC_OK);

    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});

    for (Map<String, Object> row : data) {
      assertThat(row.get("key")).isNotNull();
      assertThat(row.get("value")).isNotNull();
    }
  }

  @Test
  public void getRowsV2NotAuthorized() throws IOException {
    String keyspace = "ks_unauthorized";
    String table = "tbl1";

    String createKeyspaceRequest = String.format("{\"name\": \"%s\", \"replicas\": 1}", keyspace);

    String authToken = generateToken("cassandra", "cassandra");
    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces", host),
        createKeyspaceRequest,
        HttpStatus.SC_CREATED);

    TableAdd tableAdd = new TableAdd();
    tableAdd.setName(table);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("k", "text"));
    columnDefinitions.add(new ColumnDefinition("v", "text"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("k"));
    tableAdd.setPrimaryKey(primaryKey);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s/tables", host, keyspace),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_CREATED);

    authToken = generateToken(readOnlyUsername, readOnlyPassword);
    RestUtils.get(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspace, table, "a"),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void createTableV2NotAuthorized() throws IOException {
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName("tbl1");

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("k", "uuid"));
    columnDefinitions.add(new ColumnDefinition("v", "text"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("k"));
    tableAdd.setPrimaryKey(primaryKey);

    String authToken = generateToken(readOnlyUsername, readOnlyPassword);
    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s/tables", host, keyspaceName),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void createTableV2() throws IOException {
    TableAdd tableAdd = new TableAdd();
    tableAdd.setName("tbl2");

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new ColumnDefinition("k", "uuid"));
    columnDefinitions.add(new ColumnDefinition("v", "text"));

    tableAdd.setColumnDefinitions(columnDefinitions);

    PrimaryKey primaryKey = new PrimaryKey();
    primaryKey.setPartitionKey(Collections.singletonList("k"));
    tableAdd.setPrimaryKey(primaryKey);

    String authToken = generateToken("cassandra", "cassandra");
    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/schemas/keyspaces/%s/tables", host, keyspaceName),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_CREATED);
  }

  @Test
  public void addRowV2NotAuthorized() throws IOException {
    Map<String, String> row = new HashMap<>();
    row.put("key", "b");
    row.put("value", "bravo");

    String authToken = generateToken(readOnlyUsername, readOnlyPassword);
    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_UNAUTHORIZED);
  }

  private String generateToken(String username, String password) throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Credentials(username, password)),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    String authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();

    return authToken;
  }
}
