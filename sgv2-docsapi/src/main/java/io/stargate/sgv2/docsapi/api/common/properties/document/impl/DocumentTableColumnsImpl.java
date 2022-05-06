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

import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableColumns;
import io.stargate.sgv2.docsapi.config.DocumentConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Immutable implementation of the {@link DocumentTableColumns}.
 *
 * @see DocumentTableColumns
 */
public record DocumentTableColumnsImpl(
    List<Column> allColumns,
    String[] valueColumnNames,
    String[] pathColumnNames,
    String[] allColumnNames)
    implements DocumentTableColumns {

  // TODO check what we actually need and what data types are required
  //  arrays seem not so easy to work with

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

  private static String[] valueColumns(DocumentConfig documentConfig) {
    DocumentConfig.DocumentTableConfig table = documentConfig.table();
    return new String[] {
      table.leafColumnName(),
      table.stringValueColumnName(),
      table.doubleValueColumnName(),
      table.booleanValueColumnName()
    };
  }

  private static String[] pathColumns(DocumentConfig documentConfig) {
    DocumentConfig.DocumentTableConfig table = documentConfig.table();
    int depth = documentConfig.maxDepth();

    return IntStream.range(0, depth)
        .mapToObj(i -> table.pathColumnPrefix() + i)
        .toArray(String[]::new);
  }

  private static String[] allColumnsNames(DocumentConfig properties) {
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
    return Stream.concat(firstConcat, fixedColumns).toArray(String[]::new);
  }

  private static List<Column> allColumns(DocumentConfig config, boolean numberBooleans) {
    // TODO adapt the TypeSpecs after https://github.com/stargate/stargate/pull/1771
    // TODO add tests

    DocumentConfig.DocumentTableConfig table = config.table();
    int depth = config.maxDepth();

    List<Column> result = new ArrayList<>();

    // key column
    ImmutableColumn keyColumn =
        ImmutableColumn.builder()
            .name(table.keyColumnName())
            .type("text")
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
                        .type("text")
                        .kind(Column.Kind.CLUSTERING)
                        .build())
            .toList();
    result.addAll(pathColumns);

    // leaf column
    ImmutableColumn leaf =
        ImmutableColumn.builder().name(table.leafColumnName()).type("text").build();
    result.add(leaf);

    // string value
    ImmutableColumn stringValue =
        ImmutableColumn.builder().name(table.stringValueColumnName()).type("text").build();
    result.add(stringValue);

    // double value
    ImmutableColumn doubleValue =
        ImmutableColumn.builder().name(table.doubleValueColumnName()).type("double").build();
    result.add(doubleValue);

    // boolean value
    ImmutableColumn booleanValue;
    if (numberBooleans) {
      booleanValue =
          ImmutableColumn.builder().name(table.booleanValueColumnName()).type("tinyint").build();
    } else {
      booleanValue =
          ImmutableColumn.builder().name(table.booleanValueColumnName()).type("boolean").build();
    }
    result.add(booleanValue);

    // return unmodifiable
    return Collections.unmodifiableList(result);
  }
}
