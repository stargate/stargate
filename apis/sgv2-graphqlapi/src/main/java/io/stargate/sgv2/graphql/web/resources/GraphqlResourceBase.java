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
package io.stargate.sgv2.graphql.web.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphqlErrorException;
import graphql.com.google.common.base.MoreObjects;
import graphql.com.google.common.base.Splitter;
import graphql.com.google.common.base.Strings;
import graphql.com.google.common.collect.ImmutableList;
import graphql.com.google.common.collect.ImmutableMap;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.stargate.sgv2.graphql.web.models.GraphqlFormData;
import io.stargate.sgv2.graphql.web.models.GraphqlJsonBody;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.multipart.FileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Produces(MediaType.APPLICATION_JSON)
public abstract class GraphqlResourceBase {

  public static final String APPLICATION_GRAPHQL = "application/graphql";

  private static final Logger LOG = LoggerFactory.getLogger(GraphqlResourceBase.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Splitter PATH_SPLITTER = Splitter.on(".");

  /**
   * Handles a GraphQL GET request.
   *
   * <p>The payload is provided via URL parameters.
   */
  protected Uni<RestResponse<?>> get(
      String query, String operationName, String variables, GraphQL graphql, Object context) {

    return Uni.createFrom()
        .deferred(
            () -> {
              if (Strings.isNullOrEmpty(query)) {
                return Uni.createFrom()
                    .failure(
                        graphqlError(
                            Response.Status.BAD_REQUEST,
                            "You must provide a GraphQL query as a URL parameter"));
              }

              try {
                ExecutionInput.Builder input =
                    ExecutionInput.newExecutionInput(query).operationName(operationName);

                if (!Strings.isNullOrEmpty(variables)) {
                  @SuppressWarnings("unchecked")
                  Map<String, Object> parsedVariables =
                      OBJECT_MAPPER.readValue(variables, Map.class);
                  input = input.context(context).variables(parsedVariables);
                }

                return execute(input.build(), graphql);
              } catch (IOException e) {
                return Uni.createFrom()
                    .failure(
                        graphqlError(
                            Response.Status.BAD_REQUEST,
                            "Could not parse variables: " + e.getMessage()));
              }
            })
        // map to rest response
        .map(RestResponse::ok);
  }

  /**
   * Handles a GraphQL POST request that uses the {@link MediaType#APPLICATION_JSON} content type.
   *
   * <p>Such a request normally comprises a JSON-encoded body, but the spec also allows the query to
   * be passed as a URL parameter.
   */
  protected Uni<RestResponse<?>> postJson(
      GraphqlJsonBody jsonBody, String queryFromUrl, GraphQL graphql, Object context) {

    return Uni.createFrom()
        .deferred(
            () -> {
              String queryFromUrlSafe = Strings.emptyToNull(queryFromUrl);
              String queryFromBody =
                  (jsonBody == null) ? null : Strings.emptyToNull(jsonBody.getQuery());
              String operationName =
                  (jsonBody == null) ? null : Strings.emptyToNull(jsonBody.getOperationName());
              Map<String, Object> variables = (jsonBody == null) ? null : jsonBody.getVariables();

              if (queryFromBody == null && queryFromUrlSafe == null) {
                return Uni.createFrom()
                    .failure(
                        graphqlError(
                            Response.Status.BAD_REQUEST,
                            "You must provide a GraphQL query, either as a query parameter or in the request body"));
              }

              if (queryFromBody != null && queryFromUrlSafe != null) {
                // The GraphQL spec doesn't specify what to do in this case, but it's probably
                // better to error
                // out rather than pick one arbitrarily.
                return Uni.createFrom()
                    .failure(
                        graphqlError(
                            Response.Status.BAD_REQUEST,
                            "You can't provide a GraphQL query both as a query parameter and in the request body"));
              }

              String query = MoreObjects.firstNonNull(queryFromBody, queryFromUrlSafe);
              ExecutionInput.Builder input =
                  ExecutionInput.newExecutionInput(query)
                      .operationName(operationName)
                      .context(context);
              if (variables != null) {
                input = input.variables(variables);
              }
              return execute(input.build(), graphql);
            })
        // map to rest response
        .map(RestResponse::ok);
  }

  /**
   * Handles a GraphQL POST request that uses the {@link MediaType#MULTIPART_FORM_DATA} content
   * type, allowing file arguments.
   *
   * @see GraphqlFormData
   */
  protected Uni<RestResponse<?>> postMultipartJson(
      GraphqlFormData formData, GraphQL graphql, Object context) {

    return Uni.createFrom()
        .deferred(
            () -> {
              if (formData.operations == null) {
                return Uni.createFrom()
                    .failure(
                        graphqlError(
                            Response.Status.BAD_REQUEST,
                            "Could not find GraphQL operations object. "
                                + "Make sure your multipart request includes an 'operations' part with MIME type "
                                + MediaType.APPLICATION_JSON));
              }

              bindFilesToVariables(formData);

              return postJson(
                  formData.operations,
                  // We don't allow passing the query as a URL param for this variant. The spec does
                  // not
                  // preclude it explicitly, but it's unlikely that someone would try to do that.
                  null,
                  graphql,
                  context);
            });
  }

  /**
   * Binds the file parts to their corresponding variables in the operations part.
   *
   * <p>For example, given:
   *
   * <ul>
   *   <li>an 'operations' part such as:
   *       <pre>
   *       { "query": "...", "variables": { "file1": <whatever>, "file2": <whatever> } }
   *       </pre>
   *   <li>a 'map' part such as
   *       <pre>
   *       "part1": [ "variables.file1" ], "part2": [ "variables.file2" ] }
   *       </pre>
   *   <li>two parts 'part1' and 'part2' with the contents of the corresponding files.
   * </ul>
   *
   * <p>We want to read each file part as an {@link InputStream}, and inject it in {@link
   * GraphqlJsonBody#getVariables()} at the corresponding position (overriding whatever was there).
   */
  public void bindFilesToVariables(GraphqlFormData formData) {
    Map<String, Object> variables = formData.operations.getVariables();
    for (Map.Entry<String, List<String>> entry : formData.map.entrySet()) {
      String partName = entry.getKey();
      List<String> variablePaths = entry.getValue();
      FileUpload file =
          formData.files.stream()
              .filter(f -> f.name().equals(partName))
              .findFirst()
              .orElseThrow(
                  () ->
                      GraphqlResourceBase.graphqlError(
                          Response.Status.BAD_REQUEST,
                          String.format(
                              "The 'map' part references '%s', but found no part with that name",
                              partName)));

      if (variablePaths == null || variablePaths.size() != 1) {
        // The spec allows more than one variable, but we won't use that feature and it would
        // complicate things with InputStream.
        String message =
            String.format(
                "This implementation only allows file parts to reference exactly one variable "
                    + "(offending part: '%s' with %d variables)",
                partName, variablePaths == null ? 0 : variablePaths.size());
        throw GraphqlResourceBase.graphqlError(Response.Status.BAD_REQUEST, message);
      }
      String variablePath = variablePaths.get(0);

      List<String> pathElements = PATH_SPLITTER.splitToList(variablePath);
      if (pathElements.size() != 2 && !"variables".equals(pathElements.get(0))) {
        // Again, the spec allows more complicated cases like nested variables or arrays, but we
        // won't need that so let's keep it simple for now.
        throw GraphqlResourceBase.graphqlError(
            Response.Status.BAD_REQUEST,
            String.format(
                "This implementation only allows simple variable references like 'variables.x' "
                    + "(offending reference: '%s')",
                variablePath));
      }
      String variableName = pathElements.get(1);

      try {
        variables.put(variableName, Files.newInputStream(file.uploadedFile()));
      } catch (IOException e) {
        throw GraphqlResourceBase.graphqlError(
            Response.Status.BAD_REQUEST,
            String.format("I/O error while reading part '%s'", partName));
      }
    }
  }

  /**
   * Handles a GraphQL POST request that uses the "application/graphql" content type.
   *
   * <p>The request body is the GraphQL query directly.
   */
  protected Uni<RestResponse<?>> postGraphql(String query, GraphQL graphql, Object context) {

    return Uni.createFrom()
        .deferred(
            () -> {
              if (Strings.isNullOrEmpty(query)) {
                return Uni.createFrom()
                    .failure(
                        graphqlError(
                            Response.Status.BAD_REQUEST,
                            "You must provide a GraphQL query in the request body"));
              }

              ExecutionInput input =
                  ExecutionInput.newExecutionInput(query).context(context).build();
              return execute(input, graphql);
            })
        // map to rest response
        .map(RestResponse::ok);
  }

  protected static Uni<Map<String, Object>> execute(ExecutionInput input, GraphQL graphql) {
    // execute graphql call
    return executeGraphql(input, graphql)

        // on item check if we are not maybe overloaded
        .onItem()
        .transformToUni(
            result -> {
              Object context = input.getContext();
              if (context instanceof StargateGraphqlContext
                  && ((StargateGraphqlContext) context).isOverloaded()) {
                return Uni.createFrom()
                    .failure(
                        graphqlError(Response.Status.TOO_MANY_REQUESTS, "Database is overloaded"));
              } else {
                return Uni.createFrom().item(result.toSpecification());
              }
            })

        // on failure map to web app exception
        .onFailure()
        .recoverWithUni(
            error -> {
              LOG.error("Unexpected error while processing GraphQL request", error);
              return Uni.createFrom()
                  .failure(
                      graphqlError(Response.Status.INTERNAL_SERVER_ERROR, "Internal server error"));
            });
  }

  private static Uni<ExecutionResult> executeGraphql(ExecutionInput input, GraphQL graphql) {
    // create uni from future
    return Uni.createFrom()
        .future(() -> graphql.executeAsync(input))

        // always run subscription on workers thread
        // b/c although return type is completable future
        // we are blocking inside of the graphql
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
  }

  protected static WebApplicationException graphqlError(Response.Status status, String message) {
    return new WebApplicationException(
        Response.status(status)
            .entity(
                ImmutableMap.of("errors", ImmutableList.of(ImmutableMap.of("message", message))))
            .build());
  }

  protected static WebApplicationException graphqlError(
      Response.Status status, GraphqlErrorException error) {
    return new WebApplicationException(
        Response.status(status)
            .entity(ImmutableMap.of("errors", ImmutableList.of(error.toSpecification())))
            .build());
  }
}
