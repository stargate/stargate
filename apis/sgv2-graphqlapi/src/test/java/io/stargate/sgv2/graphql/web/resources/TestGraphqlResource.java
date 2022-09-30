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

import static graphql.GraphQL.newGraphQL;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLCodeRegistry.newCodeRegistry;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLSchema.newSchema;

import com.google.common.io.CharStreams;
import graphql.GraphQL;
import graphql.GraphQLContext;
import graphql.Scalars;
import graphql.schema.DataFetcher;
import graphql.schema.FieldCoordinates;
import io.stargate.sgv2.graphql.schema.FileSupport;
import io.stargate.sgv2.graphql.web.models.GraphqlFormData;
import io.stargate.sgv2.graphql.web.models.GraphqlJsonBody;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.MultipartForm;

/**
 * A test-only resource without any Stargate-specific logic, just to cover the generic functionality
 * in {@link GraphqlResourceBase}.
 */
@Path("/test/graphql")
@Produces(MediaType.APPLICATION_JSON)
public class TestGraphqlResource extends GraphqlResourceBase {

  // A dummy schema, just to ensure that we have something to query.
  private static final GraphQL GRAPHQL =
      newGraphQL(
              newSchema()
                  .query(
                      newObject()
                          .name("Query")
                          .field(
                              newFieldDefinition()
                                  .name("greeting")
                                  .argument(newArgument().name("name").type(Scalars.GraphQLString))
                                  .type(Scalars.GraphQLString))
                          .field(
                              newFieldDefinition()
                                  .name("greetingFromFile")
                                  .argument(
                                      newArgument().name("file").type(FileSupport.UPLOAD_SCALAR))
                                  .type(Scalars.GraphQLString)))
                  .codeRegistry(
                      newCodeRegistry()
                          .dataFetcher(
                              FieldCoordinates.coordinates("Query", "greeting"),
                              (DataFetcher<String>) env -> "hello, " + env.getArgument("name"))
                          .dataFetcher(
                              FieldCoordinates.coordinates("Query", "greetingFromFile"),
                              (DataFetcher<String>)
                                  env -> {
                                    InputStream stream = env.getArgument("file");
                                    return "hello, "
                                        + CharStreams.toString(
                                            new InputStreamReader(stream, StandardCharsets.UTF_8));
                                  })
                          .build())
                  .build())
          .build();

  // Don't need anything specific in the context
  private static final GraphQLContext CONTEXT = GraphQLContext.newContext().build();

  @GET
  public void get(
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables,
      @Suspended AsyncResponse asyncResponse) {
    super.get(query, operationName, variables, GRAPHQL, CONTEXT, asyncResponse);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public void postJson(
      GraphqlJsonBody jsonBody,
      @QueryParam("query") String queryFromUrl,
      @Suspended AsyncResponse asyncResponse) {
    super.postJson(jsonBody, queryFromUrl, GRAPHQL, CONTEXT, asyncResponse);
  }

  @POST
  @Consumes(APPLICATION_GRAPHQL)
  public void postGraphql(String query, @Suspended AsyncResponse asyncResponse) {
    super.postGraphql(query, GRAPHQL, CONTEXT, asyncResponse);
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public void postMultipartJson(
      @MultipartForm GraphqlFormData formData, @Suspended AsyncResponse asyncResponse) {
    super.postMultipartJson(formData, GRAPHQL, CONTEXT, asyncResponse);
  }
}
