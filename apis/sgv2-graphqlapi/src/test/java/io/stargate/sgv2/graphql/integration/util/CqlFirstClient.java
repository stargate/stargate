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
package io.stargate.sgv2.graphql.integration.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import okhttp3.Request;
import org.apache.http.HttpStatus;

public class CqlFirstClient extends GraphqlClient {

  private final String baseUrl;
  private final String authToken;
  private final String dmlUrl;

  public CqlFirstClient(String baseUrl, String authToken) {
    this.baseUrl = baseUrl;
    this.authToken = authToken;
    this.dmlUrl = String.format("%sgraphql-schema", baseUrl);
  }

  /** Executes a GraphQL query for a keyspace, expecting a successful response. */
  public Map<String, Object> executeDmlQuery(String keyspaceId, String graphqlQuery) {
    return getGraphqlData(authToken, buildKeyspaceUrl(keyspaceId), graphqlQuery);
  }

  /**
   * Executes a GraphQL query for a keyspace, expecting a <b>single</b> GraphQL error, and assuming
   * status code 200.s
   */
  public String getDmlQueryError(String keyspaceId, String graphqlQuery) {
    return getDmlQueryError(keyspaceId, graphqlQuery, HttpStatus.SC_OK);
  }

  /** Executes a GraphQL query for a keyspace, expecting a <b>single</b> GraphQL error. */
  public String getDmlQueryError(String keyspaceId, String graphqlQuery, int expectedStatus) {
    return getGraphqlError(authToken, buildKeyspaceUrl(keyspaceId), graphqlQuery, expectedStatus);
  }

  /** Executes a GraphQL query in {@code graphql-schema}, expecting a successful response. */
  public Map<String, Object> executeDdlQuery(String query) {
    return getGraphqlData(authToken, dmlUrl, query);
  }

  /**
   * Executes a GraphQL query in {@code graphql-schema}, expecting a <b>single</b> GraphQL error.
   */
  public String getDdlQueryError(String query) {
    return getGraphqlError(authToken, dmlUrl, query, HttpStatus.SC_OK);
  }

  public String getMetrics() {
    try {
      Request.Builder requestBuilder = new Request.Builder().get().url(baseUrl + "metrics");
      okhttp3.Response response = okHttpClient.newCall(requestBuilder.build()).execute();
      assertThat(response.code()).isEqualTo(200);
      return Objects.requireNonNull(response.body()).string();
    } catch (IOException e) {
      return fail("Unexpected error while sending POST request", e);
    }
  }

  private String buildKeyspaceUrl(String keyspaceId) {
    return (keyspaceId == null)
        ? String.format("%sgraphql", baseUrl)
        : String.format("%sgraphql/%s", baseUrl, urlEncode(keyspaceId));
  }
}
