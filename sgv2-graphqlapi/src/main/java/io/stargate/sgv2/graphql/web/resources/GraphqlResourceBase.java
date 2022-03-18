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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.ExecutionInput;
import graphql.GraphQL;
import graphql.GraphqlErrorException;
import io.stargate.proto.Schema.SchemaRead.SourceApi;
import io.stargate.sgv2.common.grpc.SchemaReads;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.web.models.GraphqlJsonBody;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factors common logic to handle GraphQL queries from JAX-RS resources.
 *
 * @see <a href="https://graphql.org/learn/serving-over-http/">Serving (GraphQL) over HTTP</a>
 */
@Produces(MediaType.APPLICATION_JSON)
public class GraphqlResourceBase {

  private static final Logger LOG = LoggerFactory.getLogger(GraphqlResourceBase.class);
  protected static final String APPLICATION_GRAPHQL = "application/graphql";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, List<String>>> FILES_MAPPING_TYPE =
      new TypeReference<Map<String, List<String>>>() {};
  private static final Splitter PATH_SPLITTER = Splitter.on(".");

  @Inject protected GraphqlCache graphqlCache;

  /**
   * Handles a GraphQL GET request.
   *
   * <p>The payload is provided via URL parameters.
   */
  protected void get(
      String query,
      String operationName,
      String variables,
      GraphQL graphql,
      HttpServletRequest httpRequest,
      AsyncResponse asyncResponse,
      StargateBridgeClient bridge) {

    if (Strings.isNullOrEmpty(query)) {
      replyWithGraphqlError(
          Status.BAD_REQUEST, "You must provide a GraphQL query as a URL parameter", asyncResponse);
      return;
    }

    try {
      ExecutionInput.Builder input =
          ExecutionInput.newExecutionInput(query)
              .operationName(operationName)
              .context(new StargateGraphqlContext(httpRequest, bridge, graphqlCache));

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
   * Handles a GraphQL POST request that uses the {@link MediaType#APPLICATION_JSON} content type.
   *
   * <p>Such a request normally comprises a JSON-encoded body, but the spec also allows the query to
   * be passed as a URL parameter.
   */
  protected void postJson(
      GraphqlJsonBody jsonBody,
      String queryFromUrl,
      GraphQL graphql,
      HttpServletRequest httpRequest,
      AsyncResponse asyncResponse,
      StargateBridgeClient bridge) {

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
            .context(new StargateGraphqlContext(httpRequest, bridge, graphqlCache));
    if (variables != null) {
      input = input.variables(variables);
    }
    executeAsync(input.build(), graphql, asyncResponse);
  }

  /**
   * Handles a GraphQL POST request that uses the {@link MediaType#MULTIPART_FORM_DATA} content
   * type, allowing file arguments.
   *
   * <p>It follows the <a
   * href="https://github.com/jaydenseric/graphql-multipart-request-spec">GraphQL multipart request
   * specification</a>.
   *
   * <p>Example cURL call:
   *
   * <pre>
   * curl http://host:port/path/to/graphql \
   *   -F operations='{ "query": "query ($file: Upload!) { someQuery(file: $file) }", "variables": { "file": null } };type=application/json'
   *   -F map='{ "filePart": ["variables.file"] }'
   *   -FfilePart=@/path/to/file.txt
   * </pre>
   *
   * The first part MUST declare a JSON content type ("type=application/json" in the example above).
   *
   * <p>This method assumes that its argument come from properly annotated arguments in a Jersey
   * resource; see the existing callers for an example.
   *
   * @param jsonBody the JSON payload containing the GraphQL "operations object" to execute (eg
   *     <code>{query=..., variables=...}</code>). It's parsed from the <code>operations
   *     </code> part in the request.
   * @param allParts the whole multipart request, consisting of: the <code>operations</code> part
   *     (ignored since we've already parsed it above), a <code>map</code> part containing a map
   *     that specifies which GraphQL variable each file corresponds to, and an arbitrary number of
   *     file parts named after the keys of the files map.
   * @param graphql the GraphQL schema to use for execution.
   */
  protected void postMultipartJson(
      GraphqlJsonBody jsonBody,
      FormDataMultiPart allParts,
      GraphQL graphql,
      HttpServletRequest httpRequest,
      AsyncResponse asyncResponse,
      StargateBridgeClient bridge) {

    if (jsonBody == null) {
      replyWithGraphqlError(
          Status.BAD_REQUEST,
          "Could not find GraphQL operations object. "
              + "Make sure your multipart request includes an 'operations' part with MIME type "
              + MediaType.APPLICATION_JSON,
          asyncResponse);
      return;
    }

    if (bindFilesToVariables(jsonBody, allParts, asyncResponse)) {
      postJson(
          jsonBody,
          // We don't allow passing the query as a URL param for this variant. The spec does not
          // preclude it explicitly, but it's unlikely that someone would try to do that.
          null,
          graphql,
          httpRequest,
          asyncResponse,
          bridge);
    }
  }

  /**
   * Given:
   *
   * <ul>
   *   <li>a GraphQL query such as:
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
   *
   * @return true if the process succeeded. Otherwise an error response has already been written.
   */
  private static boolean bindFilesToVariables(
      GraphqlJsonBody jsonBody, FormDataMultiPart allParts, AsyncResponse asyncResponse) {

    Map<String, Object> variables = jsonBody.getVariables();
    FormDataBodyPart filesMappingPart = allParts.getField("map");
    if (filesMappingPart != null) {
      if (variables == null || variables.isEmpty()) {
        // We could just ignore the files but this is likely a user mistake
        replyWithGraphqlError(
            Status.BAD_REQUEST,
            "Found a 'map' part but the GraphQL query has no variables",
            asyncResponse);
        return false;
      }
      Map<String, List<String>> filesMapping;
      try {
        filesMapping = OBJECT_MAPPER.readValue(filesMappingPart.getValue(), FILES_MAPPING_TYPE);
      } catch (JsonProcessingException e) {
        replyWithGraphqlError(
            Status.BAD_REQUEST, "Could not parse map part: " + e.getMessage(), asyncResponse);
        return false;
      }
      for (Map.Entry<String, List<String>> entry : filesMapping.entrySet()) {
        String partName = entry.getKey();
        List<String> variablePaths = entry.getValue();

        FormDataBodyPart part = allParts.getField(partName);
        if (part == null) {
          replyWithGraphqlError(
              Status.BAD_REQUEST,
              String.format(
                  "The 'map' part references '%s', but found no part with that name", partName),
              asyncResponse);
          return false;
        }

        if (variablePaths == null || variablePaths.size() != 1) {
          // The spec allows more than one variable, but we won't use that feature and it would
          // complicate things with InputStream.
          replyWithGraphqlError(
              Status.BAD_REQUEST,
              String.format(
                  "This implementation only allows file parts to reference exactly one variable "
                      + "(offending part: '%s' with %d variables)",
                  partName, variablePaths == null ? 0 : variablePaths.size()),
              asyncResponse);
          return false;
        }
        String variablePath = variablePaths.get(0);

        List<String> pathElements = PATH_SPLITTER.splitToList(variablePath);
        if (pathElements.size() != 2 && !"variables".equals(pathElements.get(0))) {
          // Again, the spec allows more complicated cases like nested variables or arrays, but we
          // won't need that so let's keep it simple for now.
          replyWithGraphqlError(
              Status.BAD_REQUEST,
              String.format(
                  "This implementation only allows simple variable references like 'variables.x' "
                      + "(offending reference: '%s')",
                  variablePath),
              asyncResponse);
          return false;
        }
        String variableName = pathElements.get(1);

        variables.put(variableName, part.getEntityAs(InputStream.class));
      }
    }
    return true;
  }

  /**
   * Handles a GraphQL POST request that uses the "application/graphql" content type.
   *
   * <p>The request body is the GraphQL query directly.
   */
  protected void postGraphql(
      String query,
      GraphQL graphql,
      HttpServletRequest httpRequest,
      AsyncResponse asyncResponse,
      StargateBridgeClient bridge) {

    if (Strings.isNullOrEmpty(query)) {
      replyWithGraphqlError(
          Status.BAD_REQUEST,
          "You must provide a GraphQL query in the request body",
          asyncResponse);
      return;
    }

    ExecutionInput input =
        ExecutionInput.newExecutionInput(query)
            .context(new StargateGraphqlContext(httpRequest, bridge, graphqlCache))
            .build();
    executeAsync(input, graphql, asyncResponse);
  }

  protected static void executeAsync(
      ExecutionInput input, GraphQL graphql, @Suspended AsyncResponse asyncResponse) {
    graphql
        .executeAsync(input)
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                LOG.error("Unexpected error while processing GraphQL request", error);
                replyWithGraphqlError(
                    Status.INTERNAL_SERVER_ERROR, "Internal server error", asyncResponse);
              } else {
                StargateGraphqlContext context = (StargateGraphqlContext) input.getContext();
                if (context.isOverloaded()) {
                  replyWithGraphqlError(
                      Status.TOO_MANY_REQUESTS, "Database is overloaded", asyncResponse);
                } else {
                  asyncResponse.resume(result.toSpecification());
                }
              }
            });
  }

  protected boolean isAuthorized(String keyspaceName, StargateBridgeClient bridge) {
    return bridge.authorizeSchemaRead(SchemaReads.keyspace(keyspaceName, SourceApi.GRAPHQL));
  }

  protected static void replyWithGraphqlError(
      Status status, String message, @Suspended AsyncResponse asyncResponse) {
    asyncResponse.resume(
        Response.status(status)
            .entity(
                ImmutableMap.of("errors", ImmutableList.of(ImmutableMap.of("message", message))))
            .build());
  }

  protected static void replyWithGraphqlError(
      Status status, GraphqlErrorException error, @Suspended AsyncResponse asyncResponse) {
    asyncResponse.resume(
        Response.status(status)
            .entity(ImmutableMap.of("errors", ImmutableList.of(error.toSpecification())))
            .build());
  }
}
