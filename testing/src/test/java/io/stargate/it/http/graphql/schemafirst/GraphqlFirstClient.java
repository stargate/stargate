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
package io.stargate.it.http.graphql.schemafirst;

import com.jayway.jsonpath.JsonPath;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.graphql.GraphqlClient;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.Response;

public class GraphqlFirstClient extends GraphqlClient {

  // Note: these constants duplicate the ones from the production code's `ResourcePaths` (which is
  // not accessible from here).
  private static final String ROOT = "/graphqlv2";
  private static final String ADMIN = ROOT + "/admin";
  private static final String NAMESPACES = ROOT + "/namespace";
  private static final String FILES = ROOT + "/files";

  private final String host;
  private final String authToken;
  private final String adminUri;
  private final String cqlDirectivesUri;

  public GraphqlFirstClient(String host, String authToken) {
    this.host = host;
    this.authToken = authToken;
    this.adminUri = String.format("http://%s:8080%s", host, ADMIN);
    this.cqlDirectivesUri = String.format("http://%s:8080%s/cql_directives.graphql", host, FILES);
  }

  /**
   * Deploys new contents to a namespace, assuming no previous version existed.
   *
   * @return the resulting version.
   */
  public UUID deploySchema(String namespace, String contents) {
    return deploySchema(namespace, null, contents);
  }

  public UUID deploySchema(String namespace, String expectedVersion, String contents) {
    Map<String, Object> response =
        getGraphqlData(
            authToken, adminUri, buildDeploySchemaQuery(namespace, expectedVersion, contents));
    String version = JsonPath.read(response, "$.deploySchema.version");
    return UUID.fromString(version);
  }

  /**
   * Deploys new contents to a namespace, assuming this will produce a single GraphQL error.
   *
   * @return the message of that error.
   */
  public String getDeploySchemaError(String namespace, String expectedVersion, String contents) {
    return getGraphqlError(
        authToken, adminUri, buildDeploySchemaQuery(namespace, expectedVersion, contents));
  }

  private String buildDeploySchemaQuery(String namespace, String expectedVersion, String contents) {
    StringBuilder query = new StringBuilder("mutation { deploySchema( ");
    query.append(String.format("namespace: \"%s\" ", namespace));
    if (expectedVersion != null) {
      query.append(String.format("expectedVersion: \"%s\" ", expectedVersion));
    }
    query.append(String.format("schema: \"\"\"%s\"\"\" ", contents));
    return query.append(") { version } }").toString();
  }

  /** Returns the contents of the static cql_directives.graphql file. */
  public String getCqlDirectivesFile() {
    try {
      return RestUtils.get(authToken, cqlDirectivesUri, Response.Status.OK.getStatusCode());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public String getSchemaFile(String namespace) {
    return getSchemaFile(namespace, null);
  }

  public String getSchemaFile(String namespace, String version, int expectedStatusCode) {
    try {
      return RestUtils.get(authToken, buildSchemaFileUri(namespace, version), expectedStatusCode);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public String getSchemaFile(String namespace, String version) {
    return getSchemaFile(namespace, version, Response.Status.OK.getStatusCode());
  }

  public void expectSchemaFileStatus(String namespace, Response.Status expectedStatus) {
    expectSchemaFileStatus(namespace, null, expectedStatus);
  }

  public void expectSchemaFileStatus(
      String namespace, String version, Response.Status expectedStatus) {
    try {
      RestUtils.get(
          authToken, buildSchemaFileUri(namespace, version), expectedStatus.getStatusCode());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Object executeNamespaceQuery(String namespace, String graphqlQuery) {
    return getGraphqlData(authToken, buildNamespaceUri(namespace), graphqlQuery);
  }

  private String buildSchemaFileUri(String namespace, String version) {
    String url = String.format("http://%s:8080%s/namespace/%s.graphql", host, FILES, namespace);
    if (version != null) {
      url = url + "?version=" + version;
    }
    return url;
  }

  private String buildNamespaceUri(String namespace) {
    return String.format("http://%s:8080%s/%s", host, NAMESPACES, namespace);
  }
}
