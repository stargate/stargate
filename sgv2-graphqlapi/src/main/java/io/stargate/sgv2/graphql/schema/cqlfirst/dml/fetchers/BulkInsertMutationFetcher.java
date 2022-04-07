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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.Schema;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.ValueModifier;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BulkInsertMutationFetcher extends BulkMutationFetcher {

  public BulkInsertMutationFetcher(
      String keyspaceName, Schema.CqlTable table, NameMapping nameMapping) {
    super(keyspaceName, table, nameMapping);
  }

  @Override
  protected List<Query> buildQueries(
      DataFetchingEnvironment environment, StargateGraphqlContext context) {
    boolean ifNotExists =
        environment.containsArgument("ifNotExists")
            && environment.getArgument("ifNotExists") != null
            && (Boolean) environment.getArgument("ifNotExists");

    List<Map<String, Object>> valuesToInsert = environment.getArgument("values");
    List<Query> boundQueries = new ArrayList<>(valuesToInsert.size());
    for (Map<String, Object> value : valuesToInsert) {
      Query query =
          new QueryBuilder()
              .insertInto(keyspaceName, table.getName())
              .value(buildInsertValues(value))
              .ifNotExists(ifNotExists)
              .ttl(getTtl(environment))
              .build();

      boundQueries.add(query);
    }
    return boundQueries;
  }

  private List<ValueModifier> buildInsertValues(Map<String, Object> value) {

    List<ValueModifier> modifiers = new ArrayList<>();
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      ColumnSpec column = dbColumnGetter.getColumn(table, entry.getKey());
      modifiers.add(
          ValueModifier.set(column.getName(), toGrpcValue(column.getType(), entry.getValue())));
    }
    return modifiers;
  }
}
