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
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.sgv2.common.cql.builder.Column.Kind;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import java.util.List;
import java.util.Map;

public class AlterTableAddFetcher extends TableFetcher {

  @Override
  protected Query buildQuery(
      DataFetchingEnvironment environment, String keyspaceName, String tableName) {
    List<Map<String, Object>> toAdd = environment.getArgument("toAdd");
    if (toAdd.isEmpty()) {
      // TODO see if we can enforce that through the schema instead
      throw new IllegalArgumentException("toAdd must contain at least one element");
    }

    return new QueryBuilder()
        .alter()
        .table(keyspaceName, tableName)
        .addColumn(decodeColumns(toAdd, Kind.REGULAR))
        .build();
  }
}
