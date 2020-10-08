package io.stargate.graphql.schema.fetchers.dml;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateMutationFetcher extends MutationFetcher {

  public UpdateMutationFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence<?, ?, ?> persistence,
      AuthenticationService authenticationService) {
    super(table, nameMapping, persistence, authenticationService);
  }

  @Override
  protected String buildStatement(DataFetchingEnvironment environment, DataStore dataStore) {
    UpdateStart updateStart = QueryBuilder.update(table.keyspace(), table.name());

    if (environment.containsArgument("options") && environment.getArgument("options") != null) {
      Map<String, Object> options = environment.getArgument("options");
      if (options.containsKey("ttl") && options.get("ttl") != null) {
        updateStart = updateStart.usingTtl((Integer) options.get("ttl"));
      }
    }

    Update update =
        updateStart
            .set(buildAssignments(table, environment))
            .where(buildPkCKWhere(table, environment))
            .if_(buildIfConditions(table, environment.getArgument("ifCondition")));

    if (environment.containsArgument("ifExists")
        && environment.getArgument("ifExists") != null
        && (Boolean) environment.getArgument("ifExists")) {
      update = update.ifExists();
    }

    if (environment.containsArgument("options") && environment.getArgument("options") != null) {
      Map<String, Object> options = environment.getArgument("options");
      if (options.containsKey("consistency") && options.get("consistency") != null) {
        //
        // update.setConsistencyLevel(ConsistencyLevel.valueOf(options.get("consistency").toString()));
      }
      if (options.containsKey("serialConsistency") && options.get("consistency") != null) {
        //
        // update.setSerialConsistencyLevel(ConsistencyLevel.valueOf(options.get("serialConsistency").toString()));
      }
    }

    return update.asCql();
  }

  private List<Relation> buildPkCKWhere(Table table, DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    List<Relation> relations = new ArrayList<>();

    for (Map.Entry<String, Object> entry : value.entrySet()) {
      Column column = getColumn(table, entry.getKey());
      if (table.partitionKeyColumns().contains(column)
          || table.clusteringKeyColumns().contains(column)) {
        relations.add(
            Relation.column(column.name()).isEqualTo(toDbLiteral(column, entry.getValue())));
      }
    }
    return relations;
  }

  private List<Assignment> buildAssignments(Table table, DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    List<Assignment> assignments = new ArrayList<>();
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      Column column = getColumn(table, entry.getKey());
      if (!(table.partitionKeyColumns().contains(column)
          || table.clusteringKeyColumns().contains(column))) {
        assignments.add(Assignment.setColumn(column.name(), toDbLiteral(column, entry.getValue())));
      }
    }
    return assignments;
  }
}
