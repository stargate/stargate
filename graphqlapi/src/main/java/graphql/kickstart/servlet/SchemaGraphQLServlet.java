package graphql.kickstart.servlet;

import io.stargate.auth.AuthenticationService;
import io.stargate.coordinator.Coordinator;
import io.stargate.graphql.core.KeyspaceManagementSchema;
import io.stargate.graphql.graphqlservlet.CassandraUnboxingGraphqlErrorHandler;
import io.stargate.graphql.graphqlservlet.GraphqlCustomContextBuilder;
import graphql.kickstart.execution.GraphQLObjectMapper;
import graphql.schema.GraphQLSchema;

public class SchemaGraphQLServlet extends SimpleGraphQLHttpServlet {
    private final Coordinator coordinator;
    private final AuthenticationService authenticationService;

    public SchemaGraphQLServlet(Coordinator coordinator, AuthenticationService authenticationService) {
        this.coordinator = coordinator;
        this.authenticationService = authenticationService;
    }
    @Override
    protected GraphQLConfiguration getConfiguration() {
        return GraphQLConfiguration
                .with(createSchema())
                .with(new GraphqlCustomContextBuilder())
                .with(GraphQLObjectMapper.newBuilder()
                        .withGraphQLErrorHandler(new CassandraUnboxingGraphqlErrorHandler())
                        .build())
                .build();
    }

    private GraphQLSchema createSchema() {
        return new KeyspaceManagementSchema(coordinator, authenticationService).build().build();
    }
}
