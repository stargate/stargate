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
package io.stargate.it.http.graphql.cqlfirst;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.it.http.graphql.GraphqlClient;
import java.util.Map;
import org.apache.http.HttpStatus;

public class CqlFirstClient extends GraphqlClient {

  private final String host;
  private final String authToken;
  private final String dmlUrl;

  public CqlFirstClient(String host, String authToken) {
    this.host = host;
    this.authToken = authToken;
    this.dmlUrl = String.format("http://%s:8080/graphql-schema", host);
  }

  /** Executes a GraphQL query for a keyspace, expecting a successful response. */
  public Map<String, Object> executeDmlQuery(CqlIdentifier keyspaceId, String graphqlQuery) {
    return getGraphqlData(authToken, buildKeyspaceUrl(keyspaceId), graphqlQuery);
  }

  /**
   * Executes a GraphQL query for a keyspace, expecting a <b>single</b> GraphQL error, and assuming
   * status code 200.s
   */
  public String getDmlQueryError(CqlIdentifier keyspaceId, String graphqlQuery) {
    return getDmlQueryError(keyspaceId, graphqlQuery, HttpStatus.SC_OK);
  }

  /** Executes a GraphQL query for a keyspace, expecting a <b>single</b> GraphQL error. */
  public String getDmlQueryError(
      CqlIdentifier keyspaceId, String graphqlQuery, int expectedStatus) {
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

  private String buildKeyspaceUrl(CqlIdentifier keyspaceId) {
    return (keyspaceId == null)
        ? String.format("http://%s:8080/graphql", host)
        : String.format("http://%s:8080/graphql/%s", host, urlEncode(keyspaceId.asInternal()));
  }
}
