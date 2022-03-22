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

import graphql.GraphQL;
import graphql.GraphqlErrorException;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.http.CreateStargateBridgeClient;
import io.stargate.sgv2.graphql.web.models.GraphqlJsonBody;
import java.util.Optional;
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
@CreateStargateBridgeClient
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
      @Suspended AsyncResponse asyncResponse,
      @Context StargateBridgeClient bridge) {

    GraphQL graphql = getDefaultGraphql(bridge);
    get(query, operationName, variables, graphql, httpRequest, asyncResponse, bridge);
  }

  @GET
  @Path("/{keyspaceName}")
  public void get(
      @PathParam("keyspaceName") String keyspaceName,
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse,
      @Context StargateBridgeClient bridge) {

    GraphQL graphql = getGraphql(keyspaceName, bridge);
    get(query, operationName, variables, graphql, httpRequest, asyncResponse, bridge);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public void postJson(
      GraphqlJsonBody jsonBody,
      @QueryParam("query") String queryFromUrl,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse,
      @Context StargateBridgeClient bridge) {

    GraphQL graphql = getDefaultGraphql(bridge);
    postJson(jsonBody, queryFromUrl, graphql, httpRequest, asyncResponse, bridge);
  }

  @POST
  @Path("/{keyspaceName}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void postJson(
      @PathParam("keyspaceName") String keyspaceName,
      GraphqlJsonBody jsonBody,
      @QueryParam("query") String queryFromUrl,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse,
      @Context StargateBridgeClient bridge) {

    GraphQL graphql = getGraphql(keyspaceName, bridge);
    postJson(jsonBody, queryFromUrl, graphql, httpRequest, asyncResponse, bridge);
  }

  @POST
  @Consumes(APPLICATION_GRAPHQL)
  public void postGraphql(
      String query,
      @Context HttpServletRequest httpRequest,
      @HeaderParam("X-Cassandra-Token") String token,
      @Suspended AsyncResponse asyncResponse,
      @Context StargateBridgeClient bridge) {

    GraphQL graphql = getDefaultGraphql(bridge);
    postGraphql(query, graphql, httpRequest, asyncResponse, bridge);
  }

  @POST
  @Path("/{keyspaceName}")
  @Consumes(APPLICATION_GRAPHQL)
  public void postGraphql(
      @PathParam("keyspaceName") String keyspaceName,
      String query,
      @Context HttpServletRequest httpRequest,
      @HeaderParam("X-Cassandra-Token") String token,
      @Suspended AsyncResponse asyncResponse,
      @Context StargateBridgeClient bridge) {

    GraphQL graphql = getGraphql(keyspaceName, bridge);
    postGraphql(query, graphql, httpRequest, asyncResponse, bridge);
  }

  private GraphQL getGraphql(String keyspaceName, StargateBridgeClient bridge) {
    if (!KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches()) {
      LOG.warn("Invalid keyspace in URI, this could be an XSS attack: {}", keyspaceName);
      // Do not reflect back the value
      throw graphqlError(Status.BAD_REQUEST, "Invalid keyspace name");
    }

    if (!isAuthorized(keyspaceName, bridge)) {
      throw graphqlError(Status.UNAUTHORIZED, "Not authorized");
    }

    try {
      String decoratedKeyspaceName = bridge.decorateKeyspaceName(keyspaceName);
      Optional<GraphQL> graphql = graphqlCache.getDml(decoratedKeyspaceName, keyspaceName);
      return graphql.orElseThrow(
          () ->
              graphqlError(Status.NOT_FOUND, String.format("Unknown keyspace '%s'", keyspaceName)));
    } catch (GraphqlErrorException e) {
      throw graphqlError(Status.INTERNAL_SERVER_ERROR, e);
    } catch (Exception e) {
      LOG.error("Unexpected error while accessing keyspace {}", keyspaceName, e);
      throw graphqlError(
          Status.INTERNAL_SERVER_ERROR,
          "Unexpected error while accessing keyspace: " + e.getMessage());
    }
  }

  private GraphQL getDefaultGraphql(StargateBridgeClient bridge) {
    return graphqlCache
        .getDefaultKeyspaceName()
        .map(ks -> getGraphql(ks, bridge))
        .orElseThrow(() -> graphqlError(Status.NOT_FOUND, "No default keyspace defined"));
  }
}
