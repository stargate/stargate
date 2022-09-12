package io.stargate.sgv2.docsapi.api.v2;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.CollectionsResource;
import io.stargate.sgv2.docsapi.api.v2.schemas.namespaces.NamespacesResource;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Serves as the base class for integration tests that need to create namespace and/or collection
 * prior to running the tests.
 *
 * <p>Note that due to the way how RestAssured is configured in Quarkus tests, we are doing the
 * initialization as first tests to be run. The {@link BeforeAll} annotation can not be used, as
 * rest assured is not configured yet. See https://github.com/quarkusio/quarkus/issues/7690 for more
 * info.
 */
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
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

  @Nested
  @Order(Integer.MIN_VALUE)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class Init {

    @Test
    @Order(1)
    public void initNamespace() {
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
              });
    }

    @Test
    @Order(2)
    public void initCollection() {
      createCollection()
          .ifPresent(
              collection -> {
                String namespace =
                    createNamespace()
                        .orElseThrow(
                            () ->
                                new IllegalStateException(
                                    "A test collection %s can not be created without specifying the namespace."
                                        .formatted(collection)));

                String json =
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
                    .body(json)
                    .post(CollectionsResource.BASE_PATH, namespace)
                    .then()
                    .statusCode(201);
              });
    }
  }
}
