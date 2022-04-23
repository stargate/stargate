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

import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableColumns;
import io.stargate.sgv2.docsapi.config.DocumentConfig;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Immutable implementation of the {@link DocumentTableColumns}.
 *
 * @see DocumentTableColumns
 */
public record DocumentTableColumnsImpl(
    String[] valueColumnNames, String[] pathColumnNames, String[] allColumnNames)
    implements DocumentTableColumns {

  /**
   * Constructor that initializes all columns based on the {@link DocumentConfig}.
   *
   * @param documentConfig {@link DocumentConfig}
   */
  public DocumentTableColumnsImpl(DocumentConfig documentConfig) {
    this(valueColumns(documentConfig), pathColumns(documentConfig), allColumns(documentConfig));
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

  private static String[] allColumns(DocumentConfig properties) {
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
}
