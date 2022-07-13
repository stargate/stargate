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

package io.stargate.sgv2.docsapi.api.common.properties.document.impl;

import com.google.common.collect.ImmutableSet;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.api.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableColumns;
import io.stargate.sgv2.docsapi.config.DocumentConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.immutables.value.Value;

/** Helper for understanding the available document table columns. */
@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PRIVATE)
public interface DocumentTableColumnsImpl extends DocumentTableColumns {

  static DocumentTableColumns of(DocumentConfig documentConfig, boolean numericBoolean) {
    return new DocumentTableColumnsImplBuilder()
        .documentConfig(documentConfig)
        .numericBooleans(numericBoolean)
        .build();
  }

  @Value.Parameter
  DocumentConfig documentConfig();

  @Value.Parameter
  boolean numericBooleans();

  /** @return Value columns, including the leaf, as {@link Set}. */
  @Value.Derived
  @Override
  default Set<String> valueColumnNames() {
    DocumentConfig.DocumentTableConfig table = documentConfig().table();
    return ImmutableSet.<String>builder()
        .add(table.leafColumnName())
        .add(table.stringValueColumnName())
        .add(table.doubleValueColumnName())
        .add(table.booleanValueColumnName())
        .build();
  }
  ;

  /** @return All the JSON path columns based on the max depth as ordered {@link List}. */
  @Value.Derived
  @Override
  default List<String> pathColumnNamesList() {
    DocumentConfig.DocumentTableConfig table = documentConfig().table();
    int depth = documentConfig().maxDepth();

    return IntStream.range(0, depth).mapToObj(i -> table.pathColumnPrefix() + i).toList();
  }
  ;

  /** @return All the JSON path columns based on the max depth as {@link Set}. */
  @Value.Derived
  @Override
  default Set<String> pathColumnNames() {
    List<String> columns = pathColumnNamesList();
    return Set.copyOf(columns);
  }
  ;

  /** @return All columns as the {@link ImmutableColumn} representation. */
  @Value.Derived
  @Override
  default List<Column> allColumns() {
    DocumentConfig.DocumentTableConfig table = documentConfig().table();
    int depth = documentConfig().maxDepth();

    List<Column> result = new ArrayList<>();

    // key column
    ImmutableColumn keyColumn =
        ImmutableColumn.builder()
            .name(table.keyColumnName())
            .type(TypeSpecs.format(TypeSpecs.VARCHAR))
            .kind(Column.Kind.PARTITION_KEY)
            .build();
    result.add(keyColumn);

    // path columns
    List<ImmutableColumn> pathColumns =
        IntStream.range(0, depth)
            .mapToObj(i -> table.pathColumnPrefix() + i)
            .map(
                pathName ->
                    ImmutableColumn.builder()
                        .name(pathName)
                        .type(TypeSpecs.format(TypeSpecs.VARCHAR))
                        .kind(Column.Kind.CLUSTERING)
                        .build())
            .toList();
    result.addAll(pathColumns);

    // leaf column
    ImmutableColumn leaf =
        ImmutableColumn.builder()
            .name(table.leafColumnName())
            .type(TypeSpecs.format(TypeSpecs.VARCHAR))
            .build();
    result.add(leaf);

    // string value
    ImmutableColumn stringValue =
        ImmutableColumn.builder()
            .name(table.stringValueColumnName())
            .type(TypeSpecs.format(TypeSpecs.VARCHAR))
            .build();
    result.add(stringValue);

    // double value
    ImmutableColumn doubleValue =
        ImmutableColumn.builder()
            .name(table.doubleValueColumnName())
            .type(TypeSpecs.format(TypeSpecs.DOUBLE))
            .build();
    result.add(doubleValue);

    // boolean value
    ImmutableColumn booleanValue;
    if (numericBooleans()) {
      booleanValue =
          ImmutableColumn.builder()
              .name(table.booleanValueColumnName())
              .type(TypeSpecs.format(TypeSpecs.TINYINT))
              .build();
    } else {
      booleanValue =
          ImmutableColumn.builder()
              .name(table.booleanValueColumnName())
              .type(TypeSpecs.format(TypeSpecs.BOOLEAN))
              .build();
    }
    result.add(booleanValue);

    // return unmodifiable
    return Collections.unmodifiableList(result);
  }
  ;

  /** @return all column names */
  @Value.Derived
  @Override
  default String[] allColumnNamesArray() {
    return allColumns().stream().map(Column::name).toArray(String[]::new);
  }
  ;
}
