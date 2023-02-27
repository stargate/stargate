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
import io.stargate.auth.Scope;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;

public class DropIndexFetcher extends IndexFetcher {

  public DropIndexFetcher() {
    super(Scope.DROP);
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment environment,
      QueryBuilder builder,
      String keyspaceName,
      String tableName) {

    String indexName = environment.getArgument("indexName");
    boolean ifExists = environment.getArgumentOrDefault("ifExists", Boolean.FALSE);

    return builder.drop().index(keyspaceName, indexName).ifExists(ifExists).build();
  }
}
