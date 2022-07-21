package io.stargate.it.http.docsapi.compatibility;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.http.RestUtils;
import java.io.IOException;

public class DocumentsV1ToV2Compat {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static String authToken;
  private static String v2Host;
  private static String v2Port;
  private static String v1Host;
  private static String v1Port;
  private static String namespace = "test_namespace";
  private static String collection = "test_collection";
  private static String documentId = "test_document";

  /*
   * This will:
   * 1) Create a document using the Documents API V1
   * 2) Get that document using Documents API V2
   * 3) Update the document using Documents API V2 and check that it was updated
   * 4) Patch a sub-document using Documents API V2 and check that it was updated
   * 5) Delete the document using Documents API V2 and check that it no longer exists
   * 6) Get the document using Documents API V1 and check that it no longer exists
   */
  public static void main(String[] args) throws IOException {
    v1Host = System.getProperty("doccapi.v1.host", "127.0.0.1");
    v1Port = System.getProperty("doccapi.v1.port", "8083");
    v2Host = System.getProperty("doccapi.v2.host", "127.0.0.1");
    v2Port = System.getProperty("doccapi.v2.port", "8180");
    authToken = RestUtils.getAuthToken(v2Host);
    String document = "{\"a\": {}, \"b\": [], \"c\": true, \"d\": {\"nested\": \"value\"}}";
    // Create a document using V1
    RestUtils.put(
        authToken,
        String.format(
            "http://%s:%s/v2/namespaces/%s/collections/%s/%s",
            v1Host, v1Port, namespace, collection, documentId),
        document,
        200);
    // Get using documents API V2
    String resp =
        RestUtils.get(
            authToken,
            String.format(
                "http://%s:%s/v2/namespaces/%s/collections/%s/%s?raw=true",
                v2Host, v2Port, namespace, collection, documentId),
            200);
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(OBJECT_MAPPER.readTree(document));
    // Update using documents API V2
    RestUtils.put(
        authToken,
        String.format(
            "http://%s:%s/v2/namespaces/%s/collections/%s/%s/a?raw=true",
            v2Host, v2Port, namespace, collection, documentId),
        "{\"new\": \"data\"}",
        200);
    resp =
        RestUtils.get(
            authToken,
            String.format(
                "http://%s:%s/v2/namespaces/%s/collections/%s/%s?raw=true",
                v2Host, v2Port, namespace, collection, documentId),
            200);

    String updated =
        "{\"a\": {\"new\": \"data\"}, \"b\": [], \"c\": true, \"d\": {\"nested\": \"value\"}}";
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(OBJECT_MAPPER.readTree(updated));

    // Patch using documents API V2
    RestUtils.patch(
        authToken,
        String.format(
            "http://%s:%s/v2/namespaces/%s/collections/%s/%s/d?raw=true",
            v2Host, v2Port, namespace, collection, documentId),
        "{\"new\": \"data\"}",
        200);

    resp =
        RestUtils.get(
            authToken,
            String.format(
                "http://%s:%s/v2/namespaces/%s/collections/%s/%s?raw=true",
                v2Host, v2Port, namespace, collection, documentId),
            200);

    String patched =
        "{\"a\": {\"new\": \"data\"}, \"b\": [], \"c\": true, \"d\": {\"nested\": \"value\", \"new\": \"data\"}}";
    assertThat(OBJECT_MAPPER.readTree(resp)).isEqualTo(OBJECT_MAPPER.readTree(patched));

    // Delete using documents API V2
    RestUtils.delete(
        authToken,
        String.format(
            "http://%s:%s/v2/namespaces/%s/collections/%s/%s?raw=true",
            v2Host, v2Port, namespace, collection, documentId),
        204);

    RestUtils.get(
        authToken,
        String.format(
            "http://%s:%s/v2/namespaces/%s/collections/%s/%s?raw=true",
            v2Host, v2Port, namespace, collection, documentId),
        404);

    // And get with V1
    RestUtils.get(
        authToken,
        String.format(
            "http://%s:%s/v2/namespaces/%s/collections/%s/%s?raw=true",
            v1Host, v1Port, namespace, collection, documentId),
        404);
  }
}
