package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2UDT;
import io.stargate.sgv2.restapi.service.models.Sgv2UDTUpdateRequest;
import java.util.Arrays;
import java.util.List;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
public class RestApiV2QSchemaUserTypeIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QSchemaUserTypeIT() {
    super("udt_ks_", "udt_t_");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Create
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void udtCreateBasic() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);
    final String typeName = "udt1";

    String createUDT =
        "{\"name\": \""
            + typeName
            + "\", \"fields\":"
            + "[{\"name\":\"firstName\",\"typeDefinition\":\"text\"},"
            + "{\"name\":\"birthDate\",\"typeDefinition\":\"date\"}]}";
    String response = tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_CREATED);
    NameResponse nameResponse = readJsonAs(response, NameResponse.class);
    assertThat(nameResponse.name).isEqualTo(typeName);

    // throws error if we call again, with the same name, but ifNotExists = false
    response = tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    // Sample output:
    //  Cassandra 4.0: “Bad request: A user type with name ‘udt1’ already exists”
    //  Cassandra 3.11, DSE 6.8: “Bad request: A user type of name
    //     ks_udtCreateBasic_1643916413499.udt1 already exists”
    assertThat(apiError.description())
        .matches(String.format("Bad request: A user type .*%s.* already exists", typeName));

    // But ok if we do conditional insert:
    // don't create and don't throw exception because ifNotExists = true
    createUDT =
        "{\"name\": \"udt1\", \"ifNotExists\": true,"
            + "\"fields\":[{\"name\":\"firstName\",\"typeDefinition\":\"text\"}]}";
    response = tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_CREATED);
  }

  @Test
  public void udtCreateInvalid() {
    final String typeName = "invalid_type";
    String createUDT =
        "{\"name\": \"udt1\", \"fields\":[{\"name\":\"firstName\",\"typeDefinition\":\""
            + typeName
            + "\"}}]}";
    String response = tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).contains("Invalid JSON payload: ");

    createUDT = "{\"name\": \"udt1\", \"fields\":[]}";
    response = tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_BAD_REQUEST);
    apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).contains("There should be at least one field defined");

    createUDT = "{\"name\": \"udt1\", \"fields\":[{\"name\":\"firstName\"}]}";
    response = tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_BAD_REQUEST);
    apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description())
        .contains("Field 'name' and 'typeDefinition' must be provided");

    createUDT = "{\"name\": \"udt1\", \"fields\":[{\"typeDefinition\":\"text\"}]}";
    response = tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_BAD_REQUEST);
    apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description())
        .contains("Field 'name' and 'typeDefinition' must be provided");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Get
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void udtGetAll() {
    // First: verify no UDTs exist before test (both wrapped and raw access)
    assertThat(findAllUDTs(testKeyspaceName(), false)).isEmpty();
    assertThat(findAllUDTs(testKeyspaceName(), true)).isEmpty();

    // Then create 9 UDTs:
    final int UDT_COUNT = 9;
    String createUDT =
        "{\"name\": \"%s\", \"fields\":[{\"name\":\"firstName\",\"typeDefinition\":\"text\"}]}";
    for (int i = 0; i < UDT_COUNT; i++) {
      tryCreateUDT(testKeyspaceName(), String.format(createUDT, "udt" + i), HttpStatus.SC_CREATED);
    }

    // And find them:
    Sgv2UDT[] udts = findAllUDTs(testKeyspaceName(), true);
    assertThat(udts).hasSize(UDT_COUNT);

    // Are they to be returned in insertion/alphabetic order? Assume so
    for (int i = 0; i < udts.length; ++i) {
      assertThat(udts[i].getName()).isEqualTo("udt" + i);
      List<Sgv2UDT.UDTField> fields = udts[i].getFields();
      assertThat(fields).hasSize(1);
      assertThat(fields.get(0).getName()).isEqualTo("firstName");
      assertThat(fields.get(0).getTypeDefinition()).isEqualTo("text");
    }
  }

  @Test
  public void udtGetOne() {
    final String typeName = "test_udt_get_one";
    String createUDT =
        "{\"name\": \""
            + typeName
            + "\", \"fields\":"
            + "[{\"name\":\"arrival\",\"typeDefinition\":\"timestamp\"},"
            + "{\"name\":\"props\",\"typeDefinition\":\"frozen<map<text,text>>\"}]}";
    tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_CREATED);

    // Find both using wrapped:
    verifyGetOneType(typeName, findOneUDT(testKeyspaceName(), typeName, false));
    // And raw
    verifyGetOneType(typeName, findOneUDT(testKeyspaceName(), typeName, true));

    // and then try accessing a non-existing UDT
    verifyTypeNotFound(testKeyspaceName(), "test_udt_get_one_bogus");
  }

  private void verifyGetOneType(String typeName, Sgv2UDT udt) {
    assertThat(udt.getName()).isEqualTo(typeName);
    List<Sgv2UDT.UDTField> fields = udt.getFields();
    assertThat(fields).hasSize(2);
    assertThat(fields.get(0).getName()).isEqualTo("arrival");
    assertThat(fields.get(0).getTypeDefinition()).isEqualTo("timestamp");
    assertThat(fields.get(1).getName()).isEqualTo("props");
    assertThat(fields.get(1).getTypeDefinition()).isEqualTo("frozen<map<text, text>>");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Update
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void udtUpdateBasic() {
    final String typeName = "udt_to_update";
    // create UDT to update
    String createUDT =
        "{\"name\": \""
            + typeName
            + "\", \"fields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";
    tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_CREATED);

    // First update: add a field
    String updateUDT =
        "{\"name\": \""
            + typeName
            + "\", \"addFields\":[{\"name\":\"lastname\",\"typeDefinition\":\"text\"}]}";
    String response = tryUpdateUDT(testKeyspaceName(), updateUDT, HttpStatus.SC_OK);
    // Empty response (shouldn't it return NO_CONTENT?)

    // Second update: rename a field
    updateUDT =
        "{\"name\": \""
            + typeName
            + "\",\"renameFields\":"
            + "[{\"from\":\"firstname\",\"to\":\"name1\"}, {\"from\":\"lastname\",\"to\":\"name2\"}]}";
    tryUpdateUDT(testKeyspaceName(), updateUDT, HttpStatus.SC_OK);

    // Fetch and verify UDT:
    Sgv2UDT udt = findOneUDT(testKeyspaceName(), typeName, true);
    assertThat(udt.getName()).isEqualTo(typeName);
    List<Sgv2UDT.UDTField> fields = udt.getFields();
    assertThat(fields).hasSize(2);

    List<String> fieldNames = Arrays.asList(fields.get(0).getName(), fields.get(1).getName());
    assertThat(fieldNames).isEqualTo(Arrays.asList("name1", "name2"));
  }

  @Test
  public void udtUpdateComplex() {
    // update UDT: add and rename field
    final String typeName = "udt_to_update_complex";
    // create UDT to update
    String createUDT =
        "{\"name\": \""
            + typeName
            + "\", \"fields\":[{\"name\":\"age\",\"typeDefinition\":\"int\"}]}";
    tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_CREATED);

    Sgv2UDTUpdateRequest updateRequest = new Sgv2UDTUpdateRequest(typeName);
    updateRequest.setAddFields(Arrays.asList(new Sgv2UDT.UDTField("name", "text")));
    updateRequest.setRenameFields(
        Arrays.asList(new Sgv2UDTUpdateRequest.FieldRename("name", "firstname")));

    tryUpdateUDT(testKeyspaceName(), asJsonString(updateRequest), HttpStatus.SC_OK);

    // Verify changes
    Sgv2UDT udt = findOneUDT(testKeyspaceName(), typeName, true);
    assertThat(udt.getName()).isEqualTo(typeName);
    List<Sgv2UDT.UDTField> fields = udt.getFields();
    assertThat(fields).hasSize(2);

    List<String> fieldNames = Arrays.asList(fields.get(0).getName(), fields.get(1).getName());
    assertThat(fieldNames).isEqualTo(Arrays.asList("age", "firstname"));
  }

  @Test
  public void udtUpdateInvalid() {
    final String typeName = "udt_update_invalid";

    // create UDT
    String createUDT =
        "{\"name\": \""
            + typeName
            + "\", \"fields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";
    tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_CREATED);

    // Try to add an existing field
    String updateUDT =
        "{\"name\": \""
            + typeName
            + "\", \"addFields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";
    String response = tryUpdateUDT(testKeyspaceName(), updateUDT, HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description())
        // Further: C*3/C*4 have different failure messages
        // C*3: "Invalid argument for gRPC operation (INVALID_ARGUMENT->Bad Request):
        //   INVALID_ARGUMENT: Cannot add new field firstname to type
        //   udt_ks_udtUpdateInvalid_1662051051470.udt_update_invalid:
        //   a field of the same name already exists"
        .matches("Invalid argument.* field .*" + typeName + ".*a field.*already exists.*");

    // missing add-type and rename-type
    updateUDT =
        "{\"name\": \"udt1\", \"fields\":[{\"name\":\"firstname\",\"typeDefinition\":\"text\"}]}";
    ;
    response = tryUpdateUDT(testKeyspaceName(), updateUDT, HttpStatus.SC_BAD_REQUEST);
    apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description())
        .contains("addFields and/or renameFields is required to update an UDT");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, DELETE
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void udtDelete() {
    final String typeName = "test_udt_delete";
    String createUDT =
        "{\"name\": \""
            + typeName
            + "\", \"fields\":[{\"name\":\"firstName\",\"typeDefinition\":\"text\"}]}";
    tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_CREATED);
    // verify it's there
    Sgv2UDT udt = findOneUDT(testKeyspaceName(), typeName, true);
    assertThat(udt.getName()).isEqualTo(typeName);

    // before deleting:
    String deletePath = endpointPathForUDT(testKeyspaceName(), typeName);
    givenWithAuth().when().delete(deletePath).then().statusCode(HttpStatus.SC_NO_CONTENT);

    // and then verify it doesn't exist any more
    verifyTypeNotFound(testKeyspaceName(), typeName);

    // Then try 2 invalid cases; first, trying to delete again
    String response =
        givenWithAuth()
            .when()
            .delete(deletePath)
            .then()
            .statusCode(HttpStatus.SC_BAD_REQUEST)
            .extract()
            .asString();
    // 23-Aug-2022, tatu: Not optimal, straight gRPC error but it is what it is:
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description())
        // Further: C*3/C*4 have different failure messages
        // C*3: "Invalid argument for gRPC operation (INVALID_ARGUMENT->Bad Request):
        //   INVALID_ARGUMENT:
        //   No user type named udt_ks_udtDelete_1662051039657.test_udt_delete exists."
        // C*4: "Invalid argument for gRPC operation (INVALID_ARGUMENT->Bad Request):
        //   INVALID_ARGUMENT:
        //   Type 'udt_ks_udtDelete_1662052163437.test_udt_delete' doesn't exist"
        .matches(String.format("Invalid argument.*%s.*exist.*", typeName));

    // And then failure due to attempt at deleting Type that is in use:
    final String tableName = testTableName();
    final String typeInUse = "fullName";
    createUDT =
        "{\"name\": \""
            + typeInUse
            + "\", \"fields\":"
            + "[{\"name\":\"firstName\",\"typeDefinition\":\"text\"},"
            + "{\"name\":\"lastName\",\"typeDefinition\":\"text\"}]}";
    tryCreateUDT(testKeyspaceName(), createUDT, HttpStatus.SC_CREATED);
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "name " + typeInUse),
        Arrays.asList("id"),
        Arrays.asList());
    response =
        givenWithAuth()
            .when()
            .delete(endpointPathForUDT(testKeyspaceName(), typeInUse))
            .then()
            .statusCode(HttpStatus.SC_BAD_REQUEST)
            .extract()
            .asString();
    // As with earlier fail message, could be improved
    apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description())
        .matches(
            String.format(
                "Invalid argument.*Cannot drop user type.*%s.* as it is still used by .*",
                typeInUse));
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
  */

  protected String endpointPathForUDT(String keyspaceName, String typeName) {
    return String.format("/v2/schemas/keyspaces/%s/types/%s", keyspaceName, typeName);
  }

  protected String endpointPathForAllUDTs(String keyspaceName) {
    return String.format("/v2/schemas/keyspaces/%s/types", keyspaceName);
  }

  protected String tryCreateUDT(String keyspaceName, String udtCreate, int expectedResult) {
    return givenWithAuth()
        .contentType(ContentType.JSON)
        .body(udtCreate)
        .when()
        .post(endpointPathForAllUDTs(keyspaceName))
        .then()
        .statusCode(expectedResult)
        .extract()
        .asString();
  }

  protected String tryUpdateUDT(String keyspaceName, String udtUpdate, int expectedResult) {
    return givenWithAuth()
        .contentType(ContentType.JSON)
        .body(udtUpdate)
        .when()
        .put(endpointPathForAllUDTs(keyspaceName))
        .then()
        .statusCode(expectedResult)
        .extract()
        .asString();
  }

  protected Sgv2UDT findOneUDT(String keyspaceName, String typeName, boolean raw) {
    String response =
        givenWithAuth()
            .queryParam("raw", raw)
            .when()
            .get(endpointPathForUDT(keyspaceName, typeName))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    if (raw) {
      return readJsonAs(response, Sgv2UDT.class);
    }
    return readWrappedRESTResponse(response, Sgv2UDT.class);
  }

  protected Sgv2UDT[] findAllUDTs(String keyspaceName, boolean raw) {
    String response =
        givenWithAuth()
            .queryParam("raw", raw)
            .when()
            .get(endpointPathForAllUDTs(keyspaceName))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    if (raw) {
      return readJsonAs(response, Sgv2UDT[].class);
    }
    return readWrappedRESTResponse(response, Sgv2UDT[].class);
  }

  private void verifyTypeNotFound(String keyspaceName, String typeName) {
    String response =
        givenWithAuth()
            .queryParam("raw", true)
            .when()
            .get(endpointPathForUDT(testKeyspaceName(), typeName))
            .then()
            .statusCode(HttpStatus.SC_NOT_FOUND)
            .extract()
            .asString();
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(apiError.description())
        .matches(String.format("No definition found for .*%s.*", typeName));
  }
}
