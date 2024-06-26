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
package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.sgv2.graphql.schema.CassandraFetcher;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;

public class SingleKeyspaceFetcher extends CassandraFetcher<KeyspaceDto> {

  @Override
  protected KeyspaceDto get(DataFetchingEnvironment environment, StargateGraphqlContext context) {
    String keyspaceName = environment.getArgument("name");

    return context
        .getBridge()
        .getKeyspace(keyspaceName, true)
        .map(keyspace -> new KeyspaceDto(keyspace, context.getBridge()))
        .orElse(null);
  }
}
