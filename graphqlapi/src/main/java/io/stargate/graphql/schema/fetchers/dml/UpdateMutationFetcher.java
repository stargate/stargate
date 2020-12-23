package io.stargate.graphql.schema.fetchers.dml;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationPrincipal;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.ValueModifier;
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
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(table, nameMapping, authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected BoundQuery buildQuery(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationPrincipal authenticationPrincipal)
      throws UnauthorizedException {
    boolean ifExists =
        environment.containsArgument("ifExists")
            && environment.getArgument("ifExists") != null
            && (Boolean) environment.getArgument("ifExists");

    BoundQuery query =
        dataStore
            .queryBuilder()
            .update(table.keyspace(), table.name())
            .ttl(getTTL(environment))
            .value(buildAssignments(table, environment))
            .where(buildPkCKWhere(table, environment))
            .ifs(buildConditions(table, environment.getArgument("ifCondition")))
            .ifExists(ifExists)
            .build()
            .bind();

    authorizationService.authorizeDataWrite(
        authenticationPrincipal,
        table.keyspace(),
        table.name(),
        TypedKeyValue.forDML((BoundDMLQuery) query),
        Scope.MODIFY,
        SourceAPI.GRAPHQL);

    return query;
  }

  private List<BuiltCondition> buildPkCKWhere(Table table, DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    List<BuiltCondition> relations = new ArrayList<>();

    for (Map.Entry<String, Object> entry : value.entrySet()) {
      Column column = getColumn(table, entry.getKey());
      if (table.partitionKeyColumns().contains(column)
          || table.clusteringKeyColumns().contains(column)) {
        relations.add(
            BuiltCondition.of(column.name(), Predicate.EQ, toDBValue(column, entry.getValue())));
      }
    }
    return relations;
  }

  private List<ValueModifier> buildAssignments(Table table, DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    List<ValueModifier> assignments = new ArrayList<>();
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      Column column = getColumn(table, entry.getKey());
      if (!(table.partitionKeyColumns().contains(column)
          || table.clusteringKeyColumns().contains(column))) {
        assignments.add(ValueModifier.set(column.name(), toDBValue(column, entry.getValue())));
      }
    }
    return assignments;
  }
}
