package io.stargate.graphql.fetchers;

import java.util.List;
import java.util.Map;
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
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

public class AlterTableAddFetcher implements io.stargate.graphql.fetchers.SchemaFetcher, DataFetcher {
    private final Persistence persistence;
    private AuthenticationService authenticationService;

    public AlterTableAddFetcher(Persistence persistence, AuthenticationService authenticationService) {
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

    public String getQuery(DataFetchingEnvironment dataFetchingEnvironment) {
        AlterTableStart start = SchemaBuilder.alterTable(
                (String)dataFetchingEnvironment.getArgument("keyspaceName"),
                (String)dataFetchingEnvironment.getArgument("tableName")
        );

        List<Map<String, Object>> toAdd = dataFetchingEnvironment.getArgument("toAdd");
        AlterTableAddColumnEnd table = null;
        for (Map<String, Object> column : toAdd) {
            if (table != null) {
                table = table.addColumn((String) column.get("name"), decodeType(column.get("type")));
            } else {
                table = start.addColumn((String) column.get("name"), decodeType(column.get("type")));
            }
        }
        return table.build().getQuery();
    }
}
