package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.enterprise.context.control.ActivateRequestContext;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QRowGetIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QRowGetIT() {
    super("rowget_ks_", "rowget_t_");
  }

  @Test
  public void getAllRowsNoPaging() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "firstName text"),
        Arrays.asList("id"),
        null);
    List<Map<String, String>> expRows =
        insertRows(
            testKeyspaceName(),
            tableName,
            Arrays.asList(
                Arrays.asList("id 1", "firstName John"),
                Arrays.asList("id 2", "firstName Jane"),
                Arrays.asList("id 3", "firstName Scott"),
                Arrays.asList("id 4", "firstName April")));

    // Do not use helper methods here but direct call
    final String path = endpointPathForAllRows(testKeyspaceName(), tableName);
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("fields", "id, firstName")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    ListOfMapsGetResponseWrapper wrapper = readJsonAs(response, ListOfMapsGetResponseWrapper.class);
    assertThat(wrapper.getCount()).isEqualTo(4);
    List<Map<String, Object>> actualRows = wrapper.getData();

    // Alas, due to "id" as partition key, ordering is arbitrary; so need to
    // convert from List to something like Set
    assertThat(actualRows).hasSize(4);
    assertThat(new LinkedHashSet<>(actualRows)).isEqualTo(new LinkedHashSet<>(expRows));
  }

  @Test
  public void getAllRowsWithPaging() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "firstName text"),
        Arrays.asList("id"),
        null);

    List<Map<String, String>> expRows =
        insertRows(
            testKeyspaceName(),
            tableName,
            Arrays.asList(
                Arrays.asList("id 1", "firstName John"),
                Arrays.asList("id 2", "firstName Jane"),
                Arrays.asList("id 3", "firstName Scott"),
                Arrays.asList("id 4", "firstName April")));

    final List<Map<String, Object>> allRows = new ArrayList<>();
    // Get first page
    final String path = endpointPathForAllRows(testKeyspaceName(), tableName);
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("page-size", 2)
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    ListOfMapsGetResponseWrapper wrapper = readJsonAs(response, ListOfMapsGetResponseWrapper.class);
    assertThat(wrapper.getCount()).isEqualTo(2);
    String pageState = wrapper.getPageState();
    assertThat(pageState).isNotEmpty();
    assertThat(wrapper.getData()).hasSize(2);
    allRows.addAll(wrapper.getData());

    // Then second
    response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("page-size", 2)
            .queryParam("page-state", pageState)
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    wrapper = readJsonAs(response, ListOfMapsGetResponseWrapper.class);
    assertThat(wrapper.getCount()).isEqualTo(2);
    pageState = wrapper.getPageState();
    assertThat(pageState).isNotEmpty();
    assertThat(wrapper.getData()).hasSize(2);
    allRows.addAll(wrapper.getData());

    // Now no more pages, shouldn't get PagingState either
    response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("page-size", 2)
            .queryParam("page-state", pageState)
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    wrapper = readJsonAs(response, ListOfMapsGetResponseWrapper.class);
    assertThat(wrapper.getPageState()).isNull();
    assertThat(wrapper.getCount()).isEqualTo(0);
    assertThat(wrapper.getData()).hasSize(0);

    assertThat(new LinkedHashSet(allRows)).isEqualTo(new LinkedHashSet(expRows));
  }

  @Test
  public void getInvalidWhereClause() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final String rowIdentifier = UUID.randomUUID().toString();
    insertRow(testKeyspaceName(), tableName, map("id", rowIdentifier));

    String whereClause = "{\"invalid_field\":{\"$eq\":\"test\"}}";
    final String path = endpointPathForRowGetWith(testKeyspaceName(), tableName);
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("where", whereClause)
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_BAD_REQUEST)
            .extract()
            .asString();
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description()).contains("Invalid 'where' parameter, problem: ");
  }

  @Test
  public void getRows() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    // To try to ensure we actually find the right entry, create one other entry first
    insertRow(
        testKeyspaceName(),
        tableName,
        map("id", UUID.randomUUID().toString(), "firstName", "Michael"));

    // and then the row we are actually looking for:
    String rowIdentifier = UUID.randomUUID().toString();
    insertRow(testKeyspaceName(), tableName, map("id", rowIdentifier, "firstName", "John"));

    ListOfMapsGetResponseWrapper wrapper =
        findRowsAsWrapped(testKeyspaceName(), tableName, rowIdentifier);
    // Verify we fetch one and only one entry
    assertThat(wrapper.getCount()).isEqualTo(1);
    List<Map<String, Object>> data = wrapper.getData();
    assertThat(data.size()).isEqualTo(1);
    // and that its contents match
    assertThat(data.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
  }

  @Test
  public void getRowsNotFound() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    insertRow(
        testKeyspaceName(),
        tableName,
        map("id", UUID.randomUUID().toString(), "firstName", "Michael"));

    final String NOT_MATCHING_ID = "f0014be3-b69f-4884-b9a6-49765fb40df3";
    ListOfMapsGetResponseWrapper wrapper =
        findRowsAsWrapped(testKeyspaceName(), tableName, NOT_MATCHING_ID);
    assertThat(wrapper.getCount()).isEqualTo(0);
    assertThat(wrapper.getData()).isEmpty();
  }

  @Test
  public void getRowsPaging() {}

  @Test
  public void getRowsPagingWithUUID() {}

  @Test
  public void getRowsPartitionAndClusterKeys() {}

  @Test
  public void getRowsPartitionKeyOnly() {}

  @Test
  public void getRowsRaw() {}

  @Test
  public void getRowsRawAndSort() {}

  @Test
  public void getRowsSort() {}

  @Test
  public void getRowsWithContainsEntryQuery() {}

  @Test
  public void getRowsWithContainsKeyQuery() {}

  @Test
  public void getRowsWithDurationValue() {}

  @Test
  public void getRowsWithExistsQuery() {}

  @Test
  public void getRowsWithInQuery() {}

  @Test
  public void getRowsWithMixedClustering() {}

  @Test
  public void getRowsWithNotFound() {}

  @Test
  public void getRowsWithQuery() {}

  @Test
  public void getRowsWithQuery2Filters() {}

  @Test
  public void getRowsWithQueryAndInvalidSort() {}

  @Test
  public void getRowsWithQueryAndPaging() {}

  @Test
  public void getRowsWithQueryAndRaw() {}

  @Test
  public void getRowsWithQueryAndSort() {}

  @Test
  public void getRowsWithQueryRawAndSort() {}

  @Test
  public void getRowsWithSetContainsQuery() {}

  @Test
  public void getRowsWithTimestampQuery() {}

  @Test
  public void getRowsWithTupleStringified() {}

  @Test
  public void getRowsWithTupleTyped() {}

  @Test
  public void getRowsWithUDT() {}
}
