package io.stargate.sgv2.docsapi.service.schema.query;

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.docsapi.config.QueriesConfig;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class JsonSchemaQueryProvider {
  @Inject QueriesConfig queriesConfig;

  /**
   * Provides a query for attaching a schema to a collection.
   *
   * @param namespace Namespace of the collection.
   * @param collection Collection name.
   * @param schema the JSON schema, stringified
   * @return Query
   */
  public QueryOuterClass.Query attachSchemaQuery(
      String namespace, String collection, String schema) {
    // parameters for the local quorum
    QueryOuterClass.QueryParameters parameters = getQueryParameters();

    return new QueryBuilder()
        .alter()
        .table(namespace, collection)
        .withComment(schema)
        .parameters(parameters)
        .build();
  }

  // constructs parameters for the queries in this provider
  private QueryOuterClass.QueryParameters getQueryParameters() {
    QueryOuterClass.Consistency consistency = queriesConfig.consistency().writes();

    return QueryOuterClass.QueryParameters.newBuilder()
        .setConsistency(QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency))
        .build();
  }
}
