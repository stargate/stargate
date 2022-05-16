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
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableColumns;
import io.stargate.sgv2.docsapi.config.DocumentConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Immutable implementation of the {@link DocumentTableColumns}.
 *
 * @see DocumentTableColumns
 */
public record DocumentTableColumnsImpl(
    List<Column> allColumns,
    Set<String> valueColumnNames,
    Set<String> pathColumnNames,
    Set<String> allColumnNames)
    implements DocumentTableColumns {

  /**
   * Constructor that initializes all columns based on the {@link DocumentConfig}.
   *
   * @param documentConfig {@link DocumentConfig}
   */
  public DocumentTableColumnsImpl(DocumentConfig documentConfig, boolean numberBooleans) {
    this(
        allColumns(documentConfig, numberBooleans),
        valueColumns(documentConfig),
        pathColumns(documentConfig),
        allColumnsNames(documentConfig));
  }

  private static Set<String> valueColumns(DocumentConfig documentConfig) {
    DocumentConfig.DocumentTableConfig table = documentConfig.table();
    return ImmutableSet.<String>builder()
        .add(table.leafColumnName())
        .add(table.stringValueColumnName())
        .add(table.doubleValueColumnName())
        .add(table.booleanValueColumnName())
        .build();
  }

  private static Set<String> pathColumns(DocumentConfig documentConfig) {
    DocumentConfig.DocumentTableConfig table = documentConfig.table();
    int depth = documentConfig.maxDepth();

    return IntStream.range(0, depth)
        .mapToObj(i -> table.pathColumnPrefix() + i)
        .collect(Collectors.toUnmodifiableSet());
  }

  private static Set<String> allColumnsNames(DocumentConfig properties) {
    DocumentConfig.DocumentTableConfig table = properties.table();
    int depth = properties.maxDepth();

    Stream<String> keyCol = Stream.of(table.keyColumnName());
    Stream<String> pColumns = IntStream.range(0, depth).mapToObj(i -> table.pathColumnPrefix() + i);
    Stream<String> fixedColumns =
        Stream.of(
            table.leafColumnName(),
            table.stringValueColumnName(),
            table.doubleValueColumnName(),
            table.booleanValueColumnName());
    Stream<String> firstConcat = Stream.concat(keyCol, pColumns);
    return Stream.concat(firstConcat, fixedColumns).collect(Collectors.toUnmodifiableSet());
  }

  private static List<Column> allColumns(DocumentConfig config, boolean numberBooleans) {
    DocumentConfig.DocumentTableConfig table = config.table();
    int depth = config.maxDepth();

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
    if (numberBooleans) {
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
}
