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
package io.stargate.graphql.web.resources.cqlfirst;

import graphql.GraphQL;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.graphql.web.RequestToHeadersMapper;
import io.stargate.graphql.web.models.GraphqlJsonBody;
import io.stargate.graphql.web.resources.GraphqlResourceBase;
import java.util.Collections;
import java.util.Map;
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
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/graphql")
@Singleton
public class GraphqlDmlResource extends GraphqlResourceBase {

  private static final Logger LOG = LoggerFactory.getLogger(GraphqlDmlResource.class);
  private static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("\\w+");

  @Inject private GraphqlCache graphqlCache;
  @Inject private AuthenticationService authenticationService;
  @Inject private AuthorizationService authorizationService;

  @GET
  public void get(
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables,
      @Context HttpServletRequest httpRequest,
      @HeaderParam("X-Cassandra-Token") String token,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getDefaultGraphql(asyncResponse, httpRequest, token);
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
      @HeaderParam("X-Cassandra-Token") String token,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getGraphl(keyspaceName, asyncResponse, httpRequest, token);
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
      @HeaderParam("X-Cassandra-Token") String token,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getDefaultGraphql(asyncResponse, httpRequest, token);
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
      @HeaderParam("X-Cassandra-Token") String token,
      @Suspended AsyncResponse asyncResponse) {

    GraphQL graphql = getGraphl(keyspaceName, asyncResponse, httpRequest, token);
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

    GraphQL graphql = getDefaultGraphql(asyncResponse, httpRequest, token);
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

    GraphQL graphql = getGraphl(keyspaceName, asyncResponse, httpRequest, token);
    if (graphql != null) {
      postGraphql(query, graphql, httpRequest, asyncResponse);
    }
  }

  private GraphQL getDefaultGraphql(
      AsyncResponse asyncResponse, HttpServletRequest request, String token) {
    GraphQL graphql = graphqlCache.getDefaultDml();
    if (graphql == null) {
      replyWithGraphqlError(Status.NOT_FOUND, "No default keyspace defined", asyncResponse);
      return null;
    } else {
      Map<String, String> headers = RequestToHeadersMapper.getAllHeaders(request);
      if (!isAuthorized(token, graphqlCache.getDefaultKeyspaceName(), headers)) {
        replyWithGraphqlError(Status.UNAUTHORIZED, "Not authorized", asyncResponse);
      }
      return graphql;
    }
  }

  private GraphQL getGraphl(
      String keyspaceName, AsyncResponse asyncResponse, HttpServletRequest request, String token) {
    if (!KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches()) {
      LOG.warn("Invalid keyspace in URI, this could be an XSS attack: {}", keyspaceName);
      // Do not reflect back the value
      replyWithGraphqlError(Status.BAD_REQUEST, "Invalid keyspace name", asyncResponse);
      return null;
    }

    Map<String, String> headers = RequestToHeadersMapper.getAllHeaders(request);
    if (!isAuthorized(token, keyspaceName, headers)) {
      replyWithGraphqlError(Status.UNAUTHORIZED, "Not authorized", asyncResponse);
    }

    GraphQL graphql = graphqlCache.getDml(keyspaceName, headers);
    if (graphql == null) {
      replyWithGraphqlError(
          Status.NOT_FOUND, String.format("Unknown keyspace '%s'", keyspaceName), asyncResponse);
      return null;
    } else {
      return graphql;
    }
  }

  private boolean isAuthorized(String token, String keyspaceName, Map<String, String> headers) {
    try {
      AuthenticationSubject authenticationSubject =
          authenticationService.validateToken(token, headers);
      authorizationService.authorizeSchemaRead(
          authenticationSubject,
          Collections.singletonList(keyspaceName),
          Collections.emptyList(),
          SourceAPI.GRAPHQL);
      return true;
    } catch (UnauthorizedException e) {
      return false;
    }
  }
}
