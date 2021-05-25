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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Kind;
import java.util.List;
import java.util.Map;

public class CreateTableFetcher extends TableFetcher {

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment environment,
      QueryBuilder builder,
      String keyspaceName,
      String tableName) {
    Boolean ifNotExists = environment.getArgument("ifNotExists");
    List<Map<String, Object>> partitionKeys = environment.getArgument("partitionKeys");
    if (partitionKeys.isEmpty()) {
      // TODO see if we can enforce that through the schema instead
      throw new IllegalArgumentException("partitionKeys must contain at least one element");
    }
    List<Map<String, Object>> clusteringKeys = environment.getArgument("clusteringKeys");
    List<Map<String, Object>> values = environment.getArgument("values");

    List<Column> partitionKeyColumns = decodeColumns(partitionKeys, Kind.PartitionKey);
    List<Column> clusteringColumns = decodeColumns(clusteringKeys, Kind.Clustering);
    List<Column> regularColumns = decodeColumns(values, Kind.Regular);
    return builder
        .create()
        .table(keyspaceName, tableName)
        .ifNotExists(ifNotExists != null && ifNotExists)
        .column(partitionKeyColumns)
        .column(clusteringColumns)
        .column(regularColumns)
        .build();
  }
}
