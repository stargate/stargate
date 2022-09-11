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
package io.stargate.sgv2.graphql.web.models;

import java.util.Map;

/** The body of a GraphQL POST request when it's using content type "application/json". */
public class GraphqlJsonBody {

  private String query;
  private String operationName;
  private Map<String, Object> variables;

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getOperationName() {
    return operationName;
  }

  public void setOperationName(String operationName) {
    this.operationName = operationName;
  }

  public Map<String, Object> getVariables() {
    return variables;
  }

  public void setVariables(Map<String, Object> variables) {
    this.variables = variables;
  }

  @Override
  public String toString() {
    return "GraphqlJsonBody{"
        + "query='"
        + query
        + '\''
        + ", operationName='"
        + operationName
        + '\''
        + ", variables="
        + variables
        + '}';
  }
}
