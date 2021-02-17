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

public class CqlFirstClient extends GraphqlClient {

  private final String host;
  private final String authToken;

  public CqlFirstClient(String host, String authToken) {
    this.host = host;
    this.authToken = authToken;
  }

  /** Executes a GraphQL query for a keyspace, expecting a successful response. */
  public Map<String, Object> getGraphqlData(CqlIdentifier keyspaceId, String graphqlQuery) {
    return getGraphqlData(authToken, buildUrl(keyspaceId), graphqlQuery);
  }

  /** Executes a GraphQL query for a keyspace, expecting a <b>single</b> GraphQL error. */
  public String getGraphqlError(CqlIdentifier keyspaceId, String graphqlQuery) {
    return getGraphqlError(authToken, buildUrl(keyspaceId), graphqlQuery);
  }

  private String buildUrl(CqlIdentifier keyspaceId) {
    return String.format("http://%s:8080/graphql/%s", host, keyspaceId.asInternal());
  }
}
