package io.stargate.graphql.fetchers;

import java.util.concurrent.CompletableFuture;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

public class DropTableFetcher implements io.stargate.graphql.fetchers.SchemaFetcher, DataFetcher {
    private final Persistence persistence;
    private AuthenticationService authenticationService;

    public DropTableFetcher(Persistence persistence, AuthenticationService authenticationService) {
        this.persistence = persistence;
        this.authenticationService = authenticationService;
    }

    @Override
    public Object get(DataFetchingEnvironment environment) throws Exception {
        HTTPAwareContextImpl httpAwareContext = environment.getContext();

        String token = httpAwareContext.getAuthToken();
        StoredCredentials storedCredentials = authenticationService.validateToken(token);
        ClientState clientState = persistence.newClientState(storedCredentials.getRoleName());
        QueryState queryState = persistence.newQueryState(clientState);
        DataStore dataStore = persistence.newDataStore(queryState, null);

        CompletableFuture<ResultSet> resultSetSingle = dataStore.query(getQuery(environment));
        resultSetSingle.get();
        return true;
    }

    @Override
    public String getQuery(DataFetchingEnvironment dataFetchingEnvironment) {
        Drop drop = SchemaBuilder.dropTable(
                (String)dataFetchingEnvironment.getArgument("keyspaceName"),
                (String)dataFetchingEnvironment.getArgument("tableName")
        );

        Boolean ifExists = dataFetchingEnvironment.getArgument("ifExists");
        if (ifExists != null && ifExists) {
            return drop.ifExists().build().getQuery();
        }
        return drop.build().getQuery();
    }
}
