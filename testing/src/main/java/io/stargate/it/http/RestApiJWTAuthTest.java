package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.KeycloakContainer;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import io.stargate.web.models.Keyspace;
import io.stargate.web.restapi.models.ColumnDefinition;
import io.stargate.web.restapi.models.GetResponseWrapper;
import io.stargate.web.restapi.models.PrimaryKey;
import io.stargate.web.restapi.models.RESTResponseWrapper;
import io.stargate.web.restapi.models.TableAdd;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE ROLE IF NOT EXISTS 'web_user' WITH PASSWORD = 'web_user' AND LOGIN = TRUE",
      "CREATE KEYSPACE IF NOT EXISTS store1 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}",
      "CREATE TABLE IF NOT EXISTS store1.shopping_cart (userid text, item_count int, last_update_timestamp timestamp, PRIMARY KEY (userid, last_update_timestamp));",
      "INSERT INTO store1.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('9876', 2, toTimeStamp(now()))",
      "INSERT INTO store1.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('1234', 5, toTimeStamp(now()))",
      "GRANT MODIFY ON TABLE store1.shopping_cart TO web_user",
      "GRANT SELECT ON TABLE store1.shopping_cart TO web_user",
    })
@ExtendWith(ApiServiceExtension.class)
@ApiServiceSpec(parametersCustomizer = "buildApiServiceParameters")
@Testcontainers(disabledWithoutDocker = true)
public class RestApiJWTAuthTest extends BaseRestApiTest {

  private static final Logger logger = LoggerFactory.getLogger(RestApiJWTAuthTest.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final String keyspaceName = "store1";
  private final String tableName = "shopping_cart";

  private String restUrlBase;

  private static String authToken;
  private static KeycloakContainer keycloakContainer;

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) throws IOException {
    keycloakContainer = new KeycloakContainer();
    keycloakContainer.initKeycloakContainer();

    builder.enableAuth(true);
    builder.putSystemProperties("stargate.auth_id", "AuthJwtService");
    builder.putSystemProperties(
        "stargate.auth.jwt_provider_url",
        String.format(
            "%s/auth/realms/stargate/protocol/openid-connect/certs", keycloakContainer.host()));
  }

  @AfterAll
  public static void teardown() {
    keycloakContainer.stop();
  }

  @BeforeEach
  public void setup(ApiServiceConnectionInfo restApi) throws IOException {
    restUrlBase = "http://" + restApi.host() + ":" + restApi.port();

    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    authToken = keycloakContainer.generateJWT();
  }

  @Test
  public void getKeyspacesv2() throws IOException {
    String body =
        RestUtils.get(
            authToken, String.format("%s/v2/schemas/keyspaces", restUrlBase), HttpStatus.SC_OK);

    RESTResponseWrapper response = objectMapper.readValue(body, RESTResponseWrapper.class);
    List<Keyspace> keyspaces =
        objectMapper.convertValue(response.getData(), new TypeReference<List<Keyspace>>() {});
    assertThat(keyspaces)
        .anySatisfy(
            value ->
                assertThat(value).isEqualToComparingFieldByField(new Keyspace("system", null)));
  }

  @Test
  public void getRowsV2() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s/v2/keyspaces/%s/%s/%s", restUrlBase, keyspaceName, tableName, "9876"),
            HttpStatus.SC_OK);

    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});

    for (Map<String, Object> row : data) {
      assertThat(row.get("userid")).isEqualTo("9876");
      assertThat((int) row.get("item_count")).isGreaterThan(0);
      assertThat(row.get("last_update_timestamp")).isNotNull();
    }
  }

  @Disabled("SGv2 does not currently support Row Level Access Control (RLAC)")
  @Test
  public void getRowsV2NotAuthorized() throws IOException {
    RestUtils.get(
        authToken,
        String.format("%s/v2/keyspaces/%s/%s/%s", restUrlBase, keyspaceName, tableName, "1234"),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void updateRowV2() throws IOException {
    String rowIdentifier = "9876";
    String updateTimestamp = now().toString();
    addRowV2(rowIdentifier, updateTimestamp, "88");

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("item_count", "27");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s/%s/%s",
                restUrlBase,
                keyspaceName,
                tableName,
                rowIdentifier,
                URLEncoder.encode(updateTimestamp, "UTF-8")),
            objectMapper.writeValueAsString(rowUpdate),
            HttpStatus.SC_OK);

    RESTResponseWrapper responseWrapper = objectMapper.readValue(body, RESTResponseWrapper.class);
    Map<String, String> data = objectMapper.convertValue(responseWrapper.getData(), Map.class);

    assertThat(data).containsAllEntriesOf(rowUpdate);
  }

  @Disabled("SGv2 does not currently support Row Level Access Control (RLAC)")
  @Test
  public void updateRowV2NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = now().toString();
    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("userid", rowIdentifier);
    rowUpdate.put("last_update_timestamp", updateTimestamp);
    rowUpdate.put("item_count", "27");

    RestUtils.put(
        authToken,
        String.format(
            "%s/v2/keyspaces/%s/%s/%s/%s",
            restUrlBase,
            keyspaceName,
            tableName,
            rowIdentifier,
            URLEncoder.encode(updateTimestamp, "UTF-8")),
        objectMapper.writeValueAsString(rowUpdate),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Disabled("SGv2 does not currently support Row Level Access Control (RLAC)")
  @Test
  public void addRowV2NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = now().toString();
    Map<String, String> row = new HashMap<>();
    row.put("userid", rowIdentifier);
    row.put("item_count", "0");
    row.put("last_update_timestamp", updateTimestamp);

    RestUtils.post(
        authToken,
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void queryRowV2() throws IOException {
    String rowIdentifier = "9876";
    String updateTimestamp = now().toString();
    addRowV2(rowIdentifier, updateTimestamp, "99");

    String whereClause =
        String.format(
            "{\"userid\":{\"$eq\":\"%s\"},\"last_update_timestamp\":{\"$eq\":\"%s\"}}",
            rowIdentifier, updateTimestamp);
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
                restUrlBase, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.get(0).get("userid")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("item_count")).isEqualTo(99);
  }

  @Disabled("SGv2 does not currently support Row Level Access Control (RLAC)")
  @Test
  public void queryRowV2NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = now().toString();

    String whereClause =
        String.format(
            "{\"userid\":{\"$eq\":\"%s\"},\"last_update_timestamp\":{\"$eq\":\"%s\"}}",
            rowIdentifier, updateTimestamp);
    RestUtils.get(
        authToken,
        String.format(
            "%s/v2/keyspaces/%s/%s?where=%s&raw=true",
            restUrlBase, keyspaceName, tableName, whereClause),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void deleteRowV2() throws IOException {
    String rowIdentifier = "9876";
    String updateTimestamp = now().toString();
    addRowV2(rowIdentifier, updateTimestamp, "88");

    RestUtils.delete(
        authToken,
        String.format(
            "%s/v2/keyspaces/%s/%s/%s/%s",
            restUrlBase,
            keyspaceName,
            tableName,
            rowIdentifier,
            URLEncoder.encode(updateTimestamp, "UTF-8")),
        HttpStatus.SC_NO_CONTENT);
  }

  @Disabled("SGv2 does not currently support Row Level Access Control (RLAC)")
  @Test
  public void deleteRowV2NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = now().toString();

    RestUtils.delete(
        authToken,
        String.format(
            "%s/v2/keyspaces/%s/%s/%s/%s",
            restUrlBase,
            keyspaceName,
            tableName,
            rowIdentifier,
            URLEncoder.encode(updateTimestamp, "UTF-8")),
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

    RestUtils.post(
        authToken,
        String.format("%s/v2/schemas/keyspaces/%s/tables", restUrlBase, keyspaceName),
        objectMapper.writeValueAsString(tableAdd),
        HttpStatus.SC_UNAUTHORIZED);
  }

  private void addRowV2(String rowIdentifier, String updateTimestamp, String itemCount)
      throws IOException {
    Map<String, String> row = new HashMap<>();
    row.put("userid", rowIdentifier);
    row.put("item_count", itemCount);
    row.put("last_update_timestamp", updateTimestamp);

    RestUtils.post(
        authToken,
        String.format("%s/v2/keyspaces/%s/%s", restUrlBase, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);
  }
}
