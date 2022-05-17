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
package io.stargate.sgv2.graphql.schema.graphqlfirst.migration;

import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.Schema.CqlIndex;
import io.stargate.sgv2.common.cql.builder.CollectionIndexingType;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;

public class CreateIndexQuery extends MigrationQuery {

  private final String tableName;
  private final CqlIndex index;

  public CreateIndexQuery(String keyspaceName, String tableName, CqlIndex index) {
    super(keyspaceName);
    this.tableName = tableName;
    this.index = index;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public Query build() {
    return new QueryBuilder()
        .create()
        .index(index.getName())
        .on(keyspaceName, tableName)
        .column(index.getColumnName())
        .indexingType(convert(index.getIndexingType()))
        .custom(index.hasIndexingClass() ? index.getIndexingClass().getValue() : null)
        .options(index.getOptionsMap())
        .build();
  }

  @Override
  public String getDescription() {
    return String.format(
        "Create index %s on %s.%s", index.getName(), tableName, index.getColumnName());
  }

  @Override
  public boolean mustRunBefore(MigrationQuery that) {
    return false;
  }

  @Override
  protected boolean addsReferenceTo(String udtName) {
    return false;
  }

  @Override
  protected boolean dropsReferenceTo(String udtName) {
    return false;
  }

  private static CollectionIndexingType convert(Schema.IndexingType indexingType) {
    switch (indexingType) {
      case DEFAULT:
        return null;
      case KEYS:
        return CollectionIndexingType.KEYS;
      case VALUES_:
        return CollectionIndexingType.VALUES;
      case ENTRIES:
        return CollectionIndexingType.ENTRIES;
      case FULL:
        return CollectionIndexingType.FULL;
      default:
        throw new IllegalArgumentException("Unsupported indexing type " + indexingType);
    }
  }
}
