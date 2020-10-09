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
package graphql.kickstart.servlet;

import graphql.kickstart.execution.GraphQLObjectMapper;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.graphql.graphqlservlet.GraphqlCustomContextBuilder;
import io.stargate.graphql.graphqlservlet.StargateGraphqlErrorHandler;
import io.stargate.graphql.schema.SchemaFactory;

public class SchemaGraphQLServlet extends SimpleGraphQLHttpServlet {
  private final Persistence persistence;
  private final AuthenticationService authenticationService;

  public SchemaGraphQLServlet(
      Persistence persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
  }

  @Override
  protected GraphQLConfiguration getConfiguration() {
    return GraphQLConfiguration.with(createSchema())
        .with(new GraphqlCustomContextBuilder())
        .with(
            GraphQLObjectMapper.newBuilder()
                .withGraphQLErrorHandler(new StargateGraphqlErrorHandler())
                .build())
        .build();
  }

  private GraphQLSchema createSchema() {
    return SchemaFactory.newDdlSchema(persistence, authenticationService);
  }
}
