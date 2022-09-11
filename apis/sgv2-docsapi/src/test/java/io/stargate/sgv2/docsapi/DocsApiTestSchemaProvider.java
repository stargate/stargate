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

package io.stargate.sgv2.docsapi;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.Schema.ColumnOrderBy;
import io.stargate.bridge.proto.Schema.CqlKeyspace;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import java.util.List;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.RandomStringUtils;

// utility class that can construct the schema for the docs api tests
@Singleton
public class DocsApiTestSchemaProvider {

  private final CqlTable table;
  private final CqlKeyspace keyspace;
  private final int maxDepth;

  @Inject
  public DocsApiTestSchemaProvider(DocumentProperties documentProperties) {
    String keyspaceName = RandomStringUtils.randomAlphabetic(16).toLowerCase();
    String collectionName = RandomStringUtils.randomAlphabetic(16).toLowerCase();

    maxDepth = documentProperties.maxDepth();
    DocumentTableProperties tableProperties = documentProperties.tableProperties();

    keyspace = CqlKeyspace.newBuilder().setName(keyspaceName).build();

    CqlTable.Builder tableBuilder =
        CqlTable.newBuilder()
            .setName(collectionName)
            .addPartitionKeyColumns(
                ColumnSpec.newBuilder()
                    .setName(tableProperties.keyColumnName())
                    .setType(TypeSpecs.VARCHAR))
            .addColumns(
                ColumnSpec.newBuilder()
                    .setName(tableProperties.leafColumnName())
                    .setType(TypeSpecs.VARCHAR))
            .addColumns(
                ColumnSpec.newBuilder()
                    .setName(tableProperties.stringValueColumnName())
                    .setType(TypeSpecs.VARCHAR))
            .addColumns(
                ColumnSpec.newBuilder()
                    .setName(tableProperties.doubleValueColumnName())
                    .setType(TypeSpecs.DOUBLE))
            .addColumns(
                ColumnSpec.newBuilder()
                    .setName(tableProperties.booleanValueColumnName())
                    .setType(TypeSpecs.BOOLEAN));

    for (int i = 0; i < maxDepth; i++) {
      String columnName = tableProperties.pathColumnName(i);
      tableBuilder
          .addClusteringKeyColumns(
              ColumnSpec.newBuilder().setName(columnName).setType(TypeSpecs.VARCHAR))
          .putClusteringOrders(columnName, ColumnOrderBy.ASC);
    }

    table = tableBuilder.build();
  }

  public CqlTable getTable() {
    return table;
  }

  public CqlKeyspace getKeyspace() {
    return keyspace;
  }

  public RowWrapper getRow(ImmutableMap<String, Value> valuesMap) {
    Stream<ColumnSpec> allColumns = allColumnSpecStream();
    List<ColumnSpec> columnsInRow =
        allColumns.filter(c -> valuesMap.containsKey(c.getName())).toList();
    QueryOuterClass.Row row =
        QueryOuterClass.Row.newBuilder()
            // Make sure the values are in the same order as the columns
            .addAllValues(columnsInRow.stream().map(c -> valuesMap.get(c.getName())).toList())
            .build();
    return RowWrapper.forColumns(columnsInRow).apply(row);
  }

  public Stream<ColumnSpec> allColumnSpecStream() {
    return allColumnSpecWithPathDepthStream(maxDepth);
  }

  public Stream<ColumnSpec> allColumnSpecWithPathDepthStream(int depth) {
    Stream<ColumnSpec> allColumns =
        Streams.concat(
            table.getPartitionKeyColumnsList().stream(),
            table.getClusteringKeyColumnsList().stream().limit(depth),
            table.getColumnsList().stream(),
            table.getStaticColumnsList().stream());
    return allColumns;
  }

  public List<ColumnSpec> allColumnSpec() {
    return allColumnSpecStream().toList();
  }

  public List<ColumnSpec> allColumnSpecForPathDepth(int depth) {
    return allColumnSpecWithPathDepthStream(depth).toList();
  }
}
