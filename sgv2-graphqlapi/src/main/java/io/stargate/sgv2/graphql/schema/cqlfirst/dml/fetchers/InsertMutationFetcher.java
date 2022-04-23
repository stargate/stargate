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

import com.google.common.base.Preconditions;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.ValueModifier;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InsertMutationFetcher extends MutationFetcher {

  public InsertMutationFetcher(String keyspaceName, CqlTable table, NameMapping nameMapping) {
    super(keyspaceName, table, nameMapping);
  }

  @Override
  protected Query buildQuery(DataFetchingEnvironment environment, StargateGraphqlContext context) {

    boolean ifNotExists =
        environment.containsArgument("ifNotExists")
            && environment.getArgument("ifNotExists") != null
            && (Boolean) environment.getArgument("ifNotExists");

    return new QueryBuilder()
        .insertInto(keyspaceName, table.getName())
        .value(buildInsertValues(environment))
        .ifNotExists(ifNotExists)
        .ttl(getTtl(environment))
        .build();
  }

  private List<ValueModifier> buildInsertValues(DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    Preconditions.checkArgument(
        value != null && !value.isEmpty(), "Insert statement must contain at least one field");

    List<ValueModifier> modifiers = new ArrayList<>();
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      ColumnSpec column = dbColumnGetter.getColumn(table, entry.getKey());
      modifiers.add(
          ValueModifier.set(column.getName(), toGrpcValue(column.getType(), entry.getValue())));
    }
    return modifiers;
  }
}
