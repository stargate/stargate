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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.docsapi.service.schema.query;

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
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
    QueryOuterClass.Consistency consistency = queriesConfig.consistency().schemaChanges();

    return QueryOuterClass.QueryParameters.newBuilder()
        .setConsistency(QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency))
        .build();
  }
}
