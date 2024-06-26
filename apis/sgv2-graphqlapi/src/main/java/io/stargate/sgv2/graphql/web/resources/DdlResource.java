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
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.web.models.GraphqlJsonBody;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.RestResponse;

/**
 * A GraphQL service that allows users to execute CQL DDL queries directly (e.g. create a keyspace,
 * drop a table, etc).
 */
@Path(ResourcePaths.DDL)
@Singleton
public class DdlResource extends StargateGraphqlResourceBase {

  private final GraphQL graphql;

  @Inject
  public DdlResource(
      StargateRequestInfo requestInfo,
      ObjectMapper objectMapper,
      StargateBridgeClient bridgeClient,
      GraphqlCache graphqlCache) {
    super(requestInfo, objectMapper, bridgeClient, graphqlCache);
    this.graphql = graphqlCache.getDdl();
  }

  @GET
  public Uni<RestResponse<?>> get(
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables) {

    return get(query, operationName, variables, graphql, newContext());
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Uni<RestResponse<?>> postJson(
      GraphqlJsonBody jsonBody, @QueryParam("query") String queryFromUrl) {

    return postJson(jsonBody, queryFromUrl, graphql, newContext());
  }

  @POST
  @Consumes(APPLICATION_GRAPHQL)
  public Uni<RestResponse<?>> postGraphql(String query) {

    return postGraphql(query, graphql, newContext());
  }
}
