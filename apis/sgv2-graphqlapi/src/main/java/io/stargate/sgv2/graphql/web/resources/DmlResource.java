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
import graphql.GraphQL;
import graphql.GraphqlErrorException;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.web.models.GraphqlJsonBody;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;
import javax.inject.Inject;
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

  @Inject
  public DmlResource(
      @GrpcClient("bridge") StargateBridge stargateBridge,
      ObjectMapper objectMapper,
      StargateBridgeClient bridgeClient,
      GraphqlCache graphqlCache) {
    super(stargateBridge, objectMapper, bridgeClient, graphqlCache);
  }

  @GET
  public Uni<RestResponse<?>> get(
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables) {

    return getDefaultGraphql()
        .flatMap(graphql -> get(query, operationName, variables, graphql, newContext()));
  }

  @GET
  @Path("/{keyspaceName}")
  public Uni<RestResponse<?>> get(
      @PathParam("keyspaceName") String keyspaceName,
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables) {

    return getGraphql(keyspaceName)
        .flatMap(graphql -> get(query, operationName, variables, graphql, newContext()));
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Uni<RestResponse<?>> postJson(
      GraphqlJsonBody jsonBody, @QueryParam("query") String queryFromUrl) {

    return getDefaultGraphql()
        .flatMap(graphql -> postJson(jsonBody, queryFromUrl, graphql, newContext()));
  }

  @POST
  @Path("/{keyspaceName}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Uni<RestResponse<?>> postJson(
      @PathParam("keyspaceName") String keyspaceName,
      GraphqlJsonBody jsonBody,
      @QueryParam("query") String queryFromUrl) {

    return getGraphql(keyspaceName)
        .flatMap(graphql -> postJson(jsonBody, queryFromUrl, graphql, newContext()));
  }

  @POST
  @Consumes(APPLICATION_GRAPHQL)
  public Uni<RestResponse<?>> postGraphql(
      String query, @HeaderParam("X-Cassandra-Token") String token) {

    return getDefaultGraphql().flatMap(graphql -> postGraphql(query, graphql, newContext()));
  }

  @POST
  @Path("/{keyspaceName}")
  @Consumes(APPLICATION_GRAPHQL)
  public Uni<RestResponse<?>> postGraphql(
      @PathParam("keyspaceName") String keyspaceName,
      String query,
      @HeaderParam("X-Cassandra-Token") String token) {

    return getGraphql(keyspaceName).flatMap(graphql -> postGraphql(query, graphql, newContext()));
  }

  private Uni<GraphQL> getGraphql(String keyspaceName) {
    return Uni.createFrom()
        .deferred(
            () -> {
              if (!KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches()) {
                LOG.warn("Invalid keyspace in URI, this could be an XSS attack: {}", keyspaceName);
                // Do not reflect back the value
                return Uni.createFrom()
                    .failure(graphqlError(Status.BAD_REQUEST, "Invalid keyspace name"));
              }

              // call is authorized
              return isAuthorized(keyspaceName)
                  .flatMap(
                      authorized -> {

                        // fail if not
                        if (!authorized) {
                          return Uni.createFrom()
                              .failure(graphqlError(Status.UNAUTHORIZED, "Not authorized"));
                        }

                        return getDml(keyspaceName)

                            // if we have no graphql fail
                            .onItem()
                            .ifNull()
                            .failWith(
                                graphqlError(
                                    Status.NOT_FOUND,
                                    String.format("Unknown keyspace '%s'", keyspaceName)))

                            // on failure map to response
                            .onFailure()
                            .recoverWithUni(
                                e -> {
                                  // in case of the GraphqlErrorException use graphqlError mapper
                                  // otherwise generic exception
                                  if (e instanceof GraphqlErrorException gee) {
                                    return Uni.createFrom()
                                        .failure(graphqlError(Status.INTERNAL_SERVER_ERROR, gee));
                                  } else {
                                    LOG.error(
                                        "Unexpected error while accessing keyspace {}",
                                        keyspaceName,
                                        e);
                                    return Uni.createFrom()
                                        .failure(
                                            graphqlError(
                                                Status.INTERNAL_SERVER_ERROR,
                                                "Unexpected error while accessing keyspace: "
                                                    + e.getMessage()));
                                  }
                                });
                      });
            });
  }

  private Uni<GraphQL> getDml(String keyspaceName) {
    return Uni.createFrom()
        .optional(() -> graphqlCache.getDml(bridgeClient, keyspaceName))

        // always run subscription on workers thread
        // we are blocking inside of the graphqlCache
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
  }

  private Uni<GraphQL> getDefaultGraphql() {
    CompletionStage<Optional<String>> result =
        graphqlCache.getDefaultKeyspaceNameAsync(bridgeClient);

    // create from future
    return Uni.createFrom()
        .future(result.toCompletableFuture())

        // map optional to uni
        .flatMap(optional -> Uni.createFrom().optional(optional))

        // if we have keyspace, map to graphql
        .onItem()
        .ifNotNull()
        .transformToUni(this::getGraphql)

        // if we end up empty throw exception
        .onItem()
        .ifNull()
        .failWith(graphqlError(Status.NOT_FOUND, "No default keyspace defined"));
  }
}
