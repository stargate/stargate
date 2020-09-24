package graphql.kickstart.servlet;

import graphql.kickstart.execution.GraphQLObjectMapper;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.graphql.core.KeyspaceManagementSchema;
import io.stargate.graphql.graphqlservlet.CassandraUnboxingGraphqlErrorHandler;
import io.stargate.graphql.graphqlservlet.GraphqlCustomContextBuilder;

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
                .withGraphQLErrorHandler(new CassandraUnboxingGraphqlErrorHandler())
                .build())
        .build();
  }

  private GraphQLSchema createSchema() {
    return new KeyspaceManagementSchema(persistence, authenticationService).build().build();
  }
}
