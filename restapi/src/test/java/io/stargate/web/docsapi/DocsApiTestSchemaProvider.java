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

package io.stargate.web.docsapi;

import static io.stargate.db.schema.Column.Kind.Clustering;
import static io.stargate.db.schema.Column.Kind.PartitionKey;
import static io.stargate.db.schema.Column.Kind.Regular;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableSchema;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.web.docsapi.service.query.QueryConstants;
import org.apache.commons.lang3.RandomStringUtils;

// utility class that can construct the schema for the docs api tests
public class DocsApiTestSchemaProvider {

  private final ImmutableTable table;
  private final ImmutableKeyspace keyspace;
  private final ImmutableSchema schema;

  public DocsApiTestSchemaProvider(int maxDepth) {
    this(
        maxDepth,
        RandomStringUtils.randomAlphabetic(16).toLowerCase(),
        RandomStringUtils.randomAlphabetic(16).toLowerCase());
  }

  public DocsApiTestSchemaProvider(int maxDepth, String keyspaceName, String collectionName) {
    ImmutableTable.Builder tableBuilder =
        ImmutableTable.builder()
            .keyspace(keyspaceName)
            .name(collectionName)
            .addColumns(
                ImmutableColumn.builder()
                    .name(QueryConstants.KEY_COLUMN_NAME)
                    .type(Column.Type.Text)
                    .kind(PartitionKey)
                    .build())
            .addColumns(
                ImmutableColumn.builder()
                    .name(QueryConstants.LEAF_COLUMN_NAME)
                    .type(Column.Type.Text)
                    .kind(Regular)
                    .build())
            .addColumns(
                ImmutableColumn.builder()
                    .name(QueryConstants.STRING_VALUE_COLUMN_NAME)
                    .type(Column.Type.Text)
                    .kind(Regular)
                    .build())
            .addColumns(
                ImmutableColumn.builder()
                    .name(QueryConstants.DOUBLE_VALUE_COLUMN_NAME)
                    .type(Column.Type.Double)
                    .kind(Regular)
                    .build())
            .addColumns(
                ImmutableColumn.builder()
                    .name(QueryConstants.BOOLEAN_VALUE_COLUMN_NAME)
                    .type(Column.Type.Boolean)
                    .kind(Regular)
                    .build());

    for (int i = 0; i < maxDepth; i++) {
      ImmutableColumn column =
          ImmutableColumn.builder()
              .name(QueryConstants.P_COLUMN_NAME.apply(i))
              .type(Column.Type.Text)
              .kind(Clustering)
              .build();
      tableBuilder = tableBuilder.addColumns(column);
    }

    table = tableBuilder.build();
    keyspace = ImmutableKeyspace.builder().name(keyspaceName).addTables(table).build();
    schema = ImmutableSchema.builder().addKeyspaces(keyspace).build();
  }

  public ImmutableTable getTable() {
    return table;
  }

  public ImmutableKeyspace getKeyspace() {
    return keyspace;
  }

  public ImmutableSchema getSchema() {
    return schema;
  }
}
