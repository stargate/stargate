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
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;

public class DeleteMutationFetcher extends MutationFetcher {

  public DeleteMutationFetcher(String keyspaceName, CqlTable table, NameMapping nameMapping) {
    super(keyspaceName, table, nameMapping);
  }

  @Override
  protected Query buildQuery(DataFetchingEnvironment environment, StargateGraphqlContext context) {

    boolean ifExists =
        environment.containsArgument("ifExists")
            && environment.getArgument("ifExists") != null
            && (Boolean) environment.getArgument("ifExists");

    return new QueryBuilder()
        .delete()
        .from(keyspaceName, table.getName())
        .where(buildClause(table, environment))
        .ifs(buildConditions(environment.getArgument("ifCondition")))
        .ifExists(ifExists)
        .parameters(buildParameters(environment))
        .build();
  }
}
