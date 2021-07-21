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
import graphql.GraphqlErrorException;
import io.stargate.db.datastore.DataStore;
import io.stargate.graphql.web.RequestToHeadersMapper;
import io.stargate.graphql.web.models.GraphqlJsonBody;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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
 * A collection of GraphQL services for user keyspaces.
 *
 * <p>For each keyspace, this is either:
 *
 * <ul>
 *   <li>a custom "GraphQL-first" schema, if the user has provided their own GraphQL.
 *   <li>otherwise, a generic "CQL-first" schema generated from the existing CQL tables.
 * </ul>
 */
@Path(ResourcePaths.DML)
@Singleton
@Authenticated
public class DmlResource extends GraphqlResourceBase {

  private static final Logger LOG = LoggerFactory.getLogger(DmlResource.class);
  static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("\\w+");

  @Inject private GraphqlCache graphqlCache;

  @GET
  public void get(
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getDefaultGraphql(httpRequest, asyncResponse);
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

    GraphQL graphql = getGraphql(keyspaceName, httpRequest, asyncResponse);
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

    GraphQL graphql = getDefaultGraphql(httpRequest, asyncResponse);
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

    GraphQL graphql = getGraphql(keyspaceName, httpRequest, asyncResponse);
    if (graphql != null) {
      postJson(jsonBody, queryFromUrl, graphql, httpRequest, asyncResponse);
    }
  }

  @POST
  @Consumes(APPLICATION_GRAPHQL)
  public void postGraphql(
      String query,
      @Context HttpServletRequest httpRequest,
      @HeaderParam("X-Cassandra-Token") String token,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getDefaultGraphql(httpRequest, asyncResponse);
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
      @HeaderParam("X-Cassandra-Token") String token,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getGraphql(keyspaceName, httpRequest, asyncResponse);
    if (graphql != null) {
      postGraphql(query, graphql, httpRequest, asyncResponse);
    }
  }

  private GraphQL getGraphql(
      String keyspaceName, HttpServletRequest httpRequest, AsyncResponse asyncResponse) {
    if (!KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches()) {
      LOG.warn("Invalid keyspace in URI, this could be an XSS attack: {}", keyspaceName);
      // Do not reflect back the value
      replyWithGraphqlError(Status.BAD_REQUEST, "Invalid keyspace name", asyncResponse);
      return null;
    }

    if (!isAuthorized(httpRequest, keyspaceName)) {
      replyWithGraphqlError(Status.UNAUTHORIZED, "Not authorized", asyncResponse);
      return null;
    }

    try {
      GraphQL graphql =
          graphqlCache.getDml(
              keyspaceName,
              (DataStore) httpRequest.getAttribute(AuthenticationFilter.DATA_STORE_KEY),
              RequestToHeadersMapper.getAllHeaders(httpRequest));
      if (graphql == null) {
        replyWithGraphqlError(
            Status.NOT_FOUND, String.format("Unknown keyspace '%s'", keyspaceName), asyncResponse);
        return null;
      } else {
        return graphql;
      }
    } catch (GraphqlErrorException e) {
      replyWithGraphqlError(Response.Status.INTERNAL_SERVER_ERROR, e, asyncResponse);
      return null;
    } catch (Exception e) {
      LOG.error("Unexpected error while accessing keyspace {}", keyspaceName, e);
      replyWithGraphqlError(
          Response.Status.INTERNAL_SERVER_ERROR,
          "Unexpected error while accessing keyspace: " + e.getMessage(),
          asyncResponse);
      return null;
    }
  }

  private GraphQL getDefaultGraphql(HttpServletRequest httpRequest, AsyncResponse asyncResponse) {
    String defaultKeyspaceName = graphqlCache.getDefaultKeyspaceName();
    if (defaultKeyspaceName == null) {
      replyWithGraphqlError(Status.NOT_FOUND, "No default keyspace defined", asyncResponse);
      return null;
    } else {
      return getGraphql(defaultKeyspaceName, httpRequest, asyncResponse);
    }
  }
}
