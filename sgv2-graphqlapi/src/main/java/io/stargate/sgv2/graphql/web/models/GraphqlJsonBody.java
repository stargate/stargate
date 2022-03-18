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
