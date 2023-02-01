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
import io.stargate.sgv2.graphql.web.models.GraphqlFormData;
import io.stargate.sgv2.graphql.web.models.GraphqlJsonBody;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.BeanParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.RestResponse;

/**
 * A GraphQL service that allows users to deploy and manage custom GraphQL schemas for their
 * keyspaces.
 */
@Singleton
@Path(ResourcePaths.ADMIN)
@Produces(MediaType.APPLICATION_JSON)
public class AdminResource extends StargateGraphqlResourceBase {

  private final GraphQL graphql;

  @Inject
  public AdminResource(
      StargateRequestInfo requestInfo,
      ObjectMapper objectMapper,
      StargateBridgeClient bridgeClient,
      GraphqlCache graphqlCache) {
    super(requestInfo, objectMapper, bridgeClient, graphqlCache);
    this.graphql = graphqlCache.getSchemaFirstAdminGraphql();
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
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Uni<RestResponse<?>> postMultipartJson(@BeanParam GraphqlFormData formData) {
    return postMultipartJson(formData, graphql, newContext());
  }

  @POST
  @Consumes(APPLICATION_GRAPHQL)
  public Uni<RestResponse<?>> postGraphql(String query) {
    return postGraphql(query, graphql, newContext());
  }
}
