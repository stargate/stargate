package io.stargate.sgv2.docsapi.api.v2;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.CollectionsResource;
import io.stargate.sgv2.docsapi.api.v2.schemas.namespaces.NamespacesResource;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Serves as the base class for integration tests that need to create namespace and/or collection
 * prior to running the tests.
 *
 * <p>Note that this test uses a small workaround in {@link IntegrationTestUtils#getTestPort()} to
 * avoid issue that Quarkus is not setting-up the rest assured target port in the @BeforeAll
 * and @AfterAll methods (see https://github.com/quarkusio/quarkus/issues/7690).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DocsApiIntegrationTest {

  /**
   * @return Returns the non-empty optional if the namespace with given name should be created
   *     before all tests.
   */
  public Optional<String> createNamespace() {
    return Optional.empty();
  }

  /**
   * @return Returns the non-empty optional if the collection with given name should be created
   *     before all tests. Note that if collection should be created, the {@link #createNamespace()}
   *     must not return empty optional.
   */
  public Optional<String> createCollection() {
    return Optional.empty();
  }

  @BeforeAll
  public void initRestAssured() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @BeforeAll
  public void createNamespaceAndCollection() {
    createNamespace()
        .ifPresent(
            namespace -> {
              String json =
                  """
                  {
                    "name": "%s"
                  }
                  """
                      .formatted(namespace);

              // create
              given()
                  .contentType(ContentType.JSON)
                  .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                  .body(json)
                  .post(NamespacesResource.BASE_PATH)
                  .then()
                  .statusCode(201);

              createCollection()
                  .ifPresent(
                      collection -> {
                        String collectionJson =
                            """
                            {
                              "name": "%s"
                            }
                            """
                                .formatted(collection);

                        // create
                        given()
                            .contentType(ContentType.JSON)
                            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                            .body(collectionJson)
                            .post(CollectionsResource.BASE_PATH, namespace)
                            .then()
                            .statusCode(201);
                      });
            });
  }

  @AfterAll
  public void deleteNamespace() {
    createNamespace()
        .ifPresent(
            namespace -> {
              // delete
              given()
                  .port(IntegrationTestUtils.getTestPort())
                  .contentType(ContentType.JSON)
                  .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                  .delete(NamespacesResource.BASE_PATH + "/{namespace}", namespace)
                  .then()
                  .statusCode(204);
            });
  }
}
