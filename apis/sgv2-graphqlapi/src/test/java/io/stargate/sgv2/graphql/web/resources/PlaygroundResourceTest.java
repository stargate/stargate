package io.stargate.sgv2.graphql.web.resources;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class PlaygroundResourceTest {

  @Test
  public void playgroundWithToken() {
    String token = RandomStringUtils.randomAlphanumeric(16);

    String body =
        given()
            .header("X-Cassandra-Token", token)
            .get("/playground")
            .then()
            .statusCode(200)
            .extract()
            .body()
            .asString();

    assertThat(body).contains("tokenValue = \"%s\";".formatted(token));
  }

  @Test
  public void playgroundWithoutToken() {
    String body = given().get("/playground").then().statusCode(200).extract().body().asString();

    assertThat(body).contains("tokenValue = \"\";");
  }
}
