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
package io.stargate.graphql.web.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.ExecutionInput;
import graphql.GraphQL;
import io.stargate.graphql.web.HttpAwareContext;
import io.stargate.graphql.web.models.GraphqlJsonBody;
import io.stargate.graphql.web.resources.cqlfirst.GraphqlDdlResource;
import java.io.IOException;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factors common logic to handle GraphQL queries from JAX-RS resources.
 *
 * @see <a href="https://graphql.org/learn/serving-over-http/">Serving (GraphQL) over HTTP</a>
 */
@Produces(MediaType.APPLICATION_JSON)
public class GraphqlResourceBase {

  private static final Logger LOG = LoggerFactory.getLogger(GraphqlDdlResource.class);
  protected static final String APPLICATION_GRAPHQL = "application/graphql";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Handles a GraphQL GET request.
   *
   * <p>The payload is provided via URL parameters.
   */
  protected static void get(
      String query,
      String operationName,
      String variables,
      GraphQL graphql,
      HttpServletRequest httpRequest,
      AsyncResponse asyncResponse) {

    if (Strings.isNullOrEmpty(query)) {
      replyWithGraphqlError(
          Status.BAD_REQUEST, "You must provide a GraphQL query as a URL parameter", asyncResponse);
      return;
    }

    try {
      ExecutionInput.Builder input =
          ExecutionInput.newExecutionInput(query)
              .operationName(operationName)
              .context(new HttpAwareContext(httpRequest));

      if (!Strings.isNullOrEmpty(variables)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> parsedVariables = OBJECT_MAPPER.readValue(variables, Map.class);
        input = input.variables(parsedVariables);
      }

      executeAsync(input.build(), graphql, asyncResponse);
    } catch (IOException e) {
      replyWithGraphqlError(
          Status.BAD_REQUEST, "Could not parse variables: " + e.getMessage(), asyncResponse);
    }
  }

  /**
   * Handles a GraphQL POST request that uses the "application/json" content type.
   *
   * <p>Such a request normally comprises a JSON-encoded body, but the spec also allows the query to
   * be passed as a URL parameter.
   */
  protected static void postJson(
      GraphqlJsonBody jsonBody,
      @QueryParam("query") String queryFromUrl,
      GraphQL graphql,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    queryFromUrl = Strings.emptyToNull(queryFromUrl);
    String queryFromBody = (jsonBody == null) ? null : Strings.emptyToNull(jsonBody.getQuery());
    String operationName =
        (jsonBody == null) ? null : Strings.emptyToNull(jsonBody.getOperationName());
    Map<String, Object> variables = (jsonBody == null) ? null : jsonBody.getVariables();

    if (queryFromBody == null && queryFromUrl == null) {
      replyWithGraphqlError(
          Status.BAD_REQUEST,
          "You must provide a GraphQL query, either as a query parameter or in the request body",
          asyncResponse);
      return;
    }

    if (queryFromBody != null && queryFromUrl != null) {
      // The GraphQL spec doesn't specify what to do in this case, but it's probably better to error
      // out rather than pick one arbitrarily.
      replyWithGraphqlError(
          Status.BAD_REQUEST,
          "You can't provide a GraphQL query both as a query parameter and in the request body",
          asyncResponse);
      return;
    }

    String query = MoreObjects.firstNonNull(queryFromBody, queryFromUrl);
    ExecutionInput.Builder input =
        ExecutionInput.newExecutionInput(query)
            .operationName(operationName)
            .context(new HttpAwareContext(httpRequest));
    if (variables != null) {
      input = input.variables(variables);
    }
    executeAsync(input.build(), graphql, asyncResponse);
  }

  /**
   * Handles a GraphQL POST request that uses the "application/graphql" content type.
   *
   * <p>The request body is the GraphQL query directly.
   */
  protected static void postGraphql(
      String query,
      GraphQL graphql,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    if (Strings.isNullOrEmpty(query)) {
      replyWithGraphqlError(
          Status.BAD_REQUEST,
          "You must provide a GraphQL query in the request body",
          asyncResponse);
      return;
    }

    ExecutionInput input =
        ExecutionInput.newExecutionInput(query).context(new HttpAwareContext(httpRequest)).build();
    executeAsync(input, graphql, asyncResponse);
  }

  protected static void executeAsync(
      ExecutionInput input, GraphQL graphql, @Suspended AsyncResponse asyncResponse) {
    graphql
        .executeAsync(input)
        .thenApply(result -> asyncResponse.resume(result.toSpecification()))
        .exceptionally(
            error -> {
              LOG.error("Unexpected error while processing GraphQL request", error);
              GraphqlResourceBase.replyWithGraphqlError(
                  Status.INTERNAL_SERVER_ERROR, "Internal server error", asyncResponse);
              return null;
            });
  }

  protected static void replyWithGraphqlError(
      Status status, String message, @Suspended AsyncResponse asyncResponse) {
    asyncResponse.resume(
        Response.status(status)
            .entity(ImmutableMap.of("errors", ImmutableList.of(message)))
            .build());
  }
}
