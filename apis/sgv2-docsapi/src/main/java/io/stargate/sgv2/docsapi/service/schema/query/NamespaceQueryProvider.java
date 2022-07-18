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
import io.stargate.sgv2.api.common.config.QueriesConfig;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.api.common.cql.builder.Replication;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Provider of queries used to manage namespaces. */
@ApplicationScoped
public class NamespaceQueryProvider {

  @Inject QueriesConfig queriesConfig;

  /**
   * Provides a query for creating a namespace.
   *
   * @param namespace Namespace name.
   * @return Query
   */
  public QueryOuterClass.Query createNamespaceQuery(String namespace, Replication replication) {

    // parameters for the local quorum
    QueryOuterClass.QueryParameters parameters = getQueryParameters();

    return new QueryBuilder()
        .create()
        .keyspace(namespace)
        .ifNotExists()
        .withReplication(replication)
        .parameters(parameters)
        .build();
  }

  /**
   * Provides query for deleting the namespace.
   *
   * @param namespace Namespace name.
   * @return Query
   */
  public QueryOuterClass.Query deleteNamespaceQuery(String namespace) {
    // parameters for the local quorum
    QueryOuterClass.QueryParameters parameters = getQueryParameters();

    // TODO @Eric should this also be if not exists? see no reason why not

    // construct delete query
    return new QueryBuilder().drop().keyspace(namespace).parameters(parameters).build();
  }

  // constructs parameters for the queries in this provider
  private QueryOuterClass.QueryParameters getQueryParameters() {
    QueryOuterClass.Consistency consistency = queriesConfig.consistency().schemaChanges();

    return QueryOuterClass.QueryParameters.newBuilder()
        .setConsistency(QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency))
        .build();
  }
}
