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

import graphql.GraphQL;
import io.stargate.graphql.web.GraphqlCache;
import io.stargate.graphql.web.RequestToHeadersMapper;
import io.stargate.graphql.web.models.GraphqlJsonBody;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/graphql")
@Singleton
public class GraphqlDmlResource extends GraphqlResourceBase {

  private static final Logger LOG = LoggerFactory.getLogger(GraphqlDmlResource.class);
  private static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("\\w+");

  @Inject private GraphqlCache graphqlCache;

  @GET
  public void get(
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getDefaultGraphql(asyncResponse);
    if (graphql != null) {
      get(query, operationName, variables, graphql, httpRequest, asyncResponse);
    }
  }

  @GET
  @Path("/{keyspaceName}")
  public void get(
      @PathParam("keyspaceName") String keyspaceName,
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getGraphl(keyspaceName, asyncResponse, httpRequest);
    if (graphql != null) {
      get(query, operationName, variables, graphql, httpRequest, asyncResponse);
    }
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public void postJson(
      GraphqlJsonBody jsonBody,
      @QueryParam("query") String queryFromUrl,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getDefaultGraphql(asyncResponse);
    if (graphql != null) {
      postJson(jsonBody, queryFromUrl, graphql, httpRequest, asyncResponse);
    }
  }

  @POST
  @Path("/{keyspaceName}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void postJson(
      @PathParam("keyspaceName") String keyspaceName,
      GraphqlJsonBody jsonBody,
      @QueryParam("query") String queryFromUrl,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getGraphl(keyspaceName, asyncResponse, httpRequest);
    if (graphql != null) {
      postJson(jsonBody, queryFromUrl, graphql, httpRequest, asyncResponse);
    }
  }

  @POST
  @Consumes(APPLICATION_GRAPHQL)
  public void postGraphql(
      String query,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getDefaultGraphql(asyncResponse);
    if (graphql != null) {
      postGraphql(query, graphql, httpRequest, asyncResponse);
    }
  }

  @POST
  @Path("/{keyspaceName}")
  @Consumes(APPLICATION_GRAPHQL)
  public void postGraphql(
      @PathParam("keyspaceName") String keyspaceName,
      String query,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getGraphl(keyspaceName, asyncResponse, httpRequest);
    if (graphql != null) {
      postGraphql(query, graphql, httpRequest, asyncResponse);
    }
  }

  private GraphQL getDefaultGraphql(AsyncResponse asyncResponse) {
    GraphQL graphql = graphqlCache.getDefaultDml();
    if (graphql == null) {
      replyWithGraphqlError(Status.NOT_FOUND, "No default keyspace defined", asyncResponse);
      return null;
    } else {
      return graphql;
    }
  }

  private GraphQL getGraphl(
      String keyspaceName, AsyncResponse asyncResponse, HttpServletRequest request) {
    if (!KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches()) {
      LOG.warn("Invalid keyspace in URI, this could be an XSS attack: {}", keyspaceName);
      // Do not reflect back the value
      replyWithGraphqlError(Status.BAD_REQUEST, "Invalid keyspace name", asyncResponse);
      return null;
    }

    GraphQL graphql =
        graphqlCache.getDml(keyspaceName, RequestToHeadersMapper.getAllHeaders(request));
    if (graphql == null) {
      replyWithGraphqlError(
          Status.NOT_FOUND, String.format("Unknown keyspace '%s'", keyspaceName), asyncResponse);
      return null;
    } else {
      return graphql;
    }
  }
}
