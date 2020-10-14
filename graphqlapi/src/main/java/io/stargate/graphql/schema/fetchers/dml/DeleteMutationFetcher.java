package io.stargate.graphql.schema.fetchers.dml;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import java.util.Map;

public class DeleteMutationFetcher extends MutationFetcher {

  public DeleteMutationFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService) {
    super(table, nameMapping, persistence, authenticationService);
  }

  @Override
  protected String buildStatement(DataFetchingEnvironment environment, DataStore dataStore) {
    Delete delete =
        QueryBuilder.deleteFrom(table.keyspace(), table.name())
            .where(buildClause(table, environment))
            .if_(buildIfConditions(table, environment.getArgument("ifCondition")));

    if (environment.containsArgument("ifExists")
        && environment.getArgument("ifExists") != null
        && (Boolean) environment.getArgument("ifExists")) {
      delete = delete.ifExists();
    }

    if (environment.containsArgument("options") && environment.getArgument("options") != null) {
      Map<String, Object> options = environment.getArgument("options");
      if (options.containsKey("consistency")) {
        //
        // delete.setConsistencyLevel(ConsistencyLevel.valueOf(options.get("consistency").toString()));
      }
      if (options.containsKey("serialConsistency")) {
        //
        // delete.setSerialConsistencyLevel(ConsistencyLevel.valueOf(options.get("serialConsistency").toString()));
      }
    }

    return delete.asCql();
  }
}
