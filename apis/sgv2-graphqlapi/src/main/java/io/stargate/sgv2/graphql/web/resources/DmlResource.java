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
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.graphql.web.models.GraphqlJsonBody;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import org.jboss.resteasy.reactive.RestResponse;
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
public class DmlResource extends StargateGraphqlResourceBase {

  private static final Logger LOG = LoggerFactory.getLogger(DmlResource.class);
  static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("\\w+");

  @GET
  public Uni<RestResponse<?>> get(
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables) {

    GraphQL graphql = getDefaultGraphql();
    return get(query, operationName, variables, graphql, newContext());
  }

  @GET
  @Path("/{keyspaceName}")
  public Uni<RestResponse<?>> get(
      @PathParam("keyspaceName") String keyspaceName,
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables) {

    GraphQL graphql = getGraphql(keyspaceName);
    return get(query, operationName, variables, graphql, newContext());
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Uni<RestResponse<?>> postJson(
      GraphqlJsonBody jsonBody, @QueryParam("query") String queryFromUrl) {

    GraphQL graphql = getDefaultGraphql();
    return postJson(jsonBody, queryFromUrl, graphql, newContext());
  }

  @POST
  @Path("/{keyspaceName}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Uni<RestResponse<?>> postJson(
      @PathParam("keyspaceName") String keyspaceName,
      GraphqlJsonBody jsonBody,
      @QueryParam("query") String queryFromUrl) {

    GraphQL graphql = getGraphql(keyspaceName);
    return postJson(jsonBody, queryFromUrl, graphql, newContext());
  }

  @POST
  @Consumes(APPLICATION_GRAPHQL)
  public Uni<RestResponse<?>> postGraphql(
      String query, @HeaderParam("X-Cassandra-Token") String token) {

    GraphQL graphql = getDefaultGraphql();
    return postGraphql(query, graphql, newContext());
  }

  @POST
  @Path("/{keyspaceName}")
  @Consumes(APPLICATION_GRAPHQL)
  public Uni<RestResponse<?>> postGraphql(
      @PathParam("keyspaceName") String keyspaceName,
      String query,
      @HeaderParam("X-Cassandra-Token") String token) {

    GraphQL graphql = getGraphql(keyspaceName);
    return postGraphql(query, graphql, newContext());
  }

  private GraphQL getGraphql(String keyspaceName) {
    if (!KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches()) {
      LOG.warn("Invalid keyspace in URI, this could be an XSS attack: {}", keyspaceName);
      // Do not reflect back the value
      throw graphqlError(Status.BAD_REQUEST, "Invalid keyspace name");
    }

    if (!isAuthorized(keyspaceName)) {
      throw graphqlError(Status.UNAUTHORIZED, "Not authorized");
    }

    try {
      String decoratedKeyspaceName = bridge.decorateKeyspaceName(keyspaceName);
      Optional<GraphQL> graphql = graphqlCache.getDml(bridge, keyspaceName);
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

  private GraphQL getDefaultGraphql() {
    return graphqlCache
        .getDefaultKeyspaceName(bridge)
        .map(this::getGraphql)
        .orElseThrow(() -> graphqlError(Status.NOT_FOUND, "No default keyspace defined"));
  }
}
