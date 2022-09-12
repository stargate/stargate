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
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.api.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateTypeFetcher extends DdlQueryFetcher {

  @Override
  protected Query buildQuery(DataFetchingEnvironment environment, StargateGraphqlContext context) {

    String keyspaceName = environment.getArgument("keyspaceName");
    String typeName = environment.getArgument("typeName");

    List<Map<String, Object>> fieldList = environment.getArgument("fields");
    if (fieldList.isEmpty()) {
      throw new IllegalArgumentException("Must have at least one field");
    }
    List<Column> fields = new ArrayList<>(fieldList.size());
    for (Map<String, Object> key : fieldList) {
      fields.add(
          ImmutableColumn.builder()
              .name((String) key.get("name"))
              .type(decodeType(key.get("type")))
              .build());
    }

    Boolean ifNotExists = environment.getArgument("ifNotExists");
    return new QueryBuilder()
        .create()
        .type(keyspaceName, typeName)
        .ifNotExists(ifNotExists != null && ifNotExists)
        .column(fields)
        .build();
  }
}
