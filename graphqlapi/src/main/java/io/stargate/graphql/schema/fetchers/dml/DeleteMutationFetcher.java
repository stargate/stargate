package io.stargate.graphql.schema.fetchers.dml;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Table;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import io.stargate.graphql.schema.NameMapping;
import java.util.List;

public class DeleteMutationFetcher extends MutationFetcher {

  public DeleteMutationFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(table, nameMapping, persistence, authenticationService, authorizationService);
  }

  @Override
  protected String buildStatement(DataFetchingEnvironment environment, DataStore dataStore)
      throws Exception {
    HTTPAwareContextImpl httpAwareContext = environment.getContext();
    String token = httpAwareContext.getAuthToken();

    List<Relation> relations = buildClause(table, environment);
    authorizationService.authorizedDataWrite(
        token, buildTypedKeyValueList(relations), Scope.DELETE);

    Delete delete =
        QueryBuilder.deleteFrom(keyspaceId, tableId)
            .where(relations)
            .if_(buildIfConditions(table, environment.getArgument("ifCondition")));

    if (environment.containsArgument("ifExists")
        && environment.getArgument("ifExists") != null
        && (Boolean) environment.getArgument("ifExists")) {
      delete = delete.ifExists();
    }

    return delete.asCql();
  }
}
