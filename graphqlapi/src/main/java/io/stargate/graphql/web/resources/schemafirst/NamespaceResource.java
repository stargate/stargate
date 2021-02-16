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
package io.stargate.graphql.web.resources.schemafirst;

import graphql.GraphQL;
import io.stargate.graphql.web.RequestToHeadersMapper;
import io.stargate.graphql.web.models.GraphqlJsonBody;
import io.stargate.graphql.web.resources.GraphqlResourceBase;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Serves the GraphQL schema deployed on each namespace. */
@Singleton
@Path(ResourcePaths.NAMESPACES)
@Produces(MediaType.APPLICATION_JSON)
public class NamespaceResource extends GraphqlResourceBase {

  private static final Logger LOG = LoggerFactory.getLogger(NamespaceResource.class);
  private static final Pattern NAMESPACE_PATTERN = Pattern.compile("\\w+");

  @Inject private SchemaFirstCache graphqlCache;

  @GET
  @Path("/{namespace}")
  public void get(
      @PathParam("namespace") String namespace,
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getGraphql(namespace, httpRequest, asyncResponse);
    if (graphql != null) {
      get(query, operationName, variables, graphql, httpRequest, asyncResponse);
    }
  }

  @POST
  @Path("/{namespace}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void postJson(
      @PathParam("namespace") String namespace,
      GraphqlJsonBody jsonBody,
      @QueryParam("query") String queryFromUrl,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getGraphql(namespace, httpRequest, asyncResponse);
    if (graphql != null) {
      postJson(jsonBody, queryFromUrl, graphql, httpRequest, asyncResponse);
    }
  }

  @POST
  @Path("/{namespace}")
  @Consumes(APPLICATION_GRAPHQL)
  public void postGraphql(
      @PathParam("namespace") String namespace,
      String query,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getGraphql(namespace, httpRequest, asyncResponse);
    if (graphql != null) {
      postGraphql(query, graphql, httpRequest, asyncResponse);
    }
  }

  private GraphQL getGraphql(
      String namespace, HttpServletRequest request, AsyncResponse asyncResponse) {
    if (!NAMESPACE_PATTERN.matcher(namespace).matches()) {
      LOG.warn("Invalid namespace in URI, this could be an XSS attack: {}", namespace);
      // Do not reflect back the value
      replyWithGraphqlError(Response.Status.BAD_REQUEST, "Invalid namespace name", asyncResponse);
      return null;
    }

    try {
      GraphQL graphql =
          graphqlCache.getGraphql(namespace, RequestToHeadersMapper.getAllHeaders(request));
      if (graphql == null) {
        replyWithGraphqlError(
            Response.Status.NOT_FOUND,
            String.format(
                "Could not find a GraphQL schema for '%s', either the namespace does not exist, "
                    + "or no schema was deployed to it yet.",
                namespace),
            asyncResponse);
        return null;
      } else {
        return graphql;
      }
    } catch (Exception e) {
      LOG.error("Unexpected error while accessing namespace {}", namespace, e);
      replyWithGraphqlError(
          Response.Status.NOT_FOUND, "Unexpected error while accessing namespace", asyncResponse);
      return null;
    }
  }
}
