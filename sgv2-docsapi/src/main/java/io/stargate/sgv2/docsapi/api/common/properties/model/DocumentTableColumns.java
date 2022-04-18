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

package io.stargate.sgv2.docsapi.api.common.properties.model;

import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A helper that pre-resolves all the columns names for the document table.
 *
 * @param valueColumnNames Value columns
 * @param pathColumnNames Path columns
 * @param allColumnNames All columns
 */
public record DocumentTableColumns(
    String[] valueColumnNames, String[] pathColumnNames, String[] allColumnNames) {

  /**
   * Constructor that initializes all columns based on the {@link DocumentProperties}.
   *
   * @param properties {@link DocumentProperties}
   */
  public DocumentTableColumns(DocumentProperties properties) {
    this(valueColumns(properties), pathColumns(properties), allColumns(properties));
  }

  private static String[] valueColumns(DocumentProperties properties) {
    DocumentTableProperties table = properties.table();
    return new String[] {
      table.leafColumnName(),
      table.stringValueColumnName(),
      table.doubleValueColumnName(),
      table.booleanValueColumnName()
    };
  }

  private static String[] pathColumns(DocumentProperties properties) {
    DocumentTableProperties table = properties.table();
    int depth = properties.maxDepth();

    return IntStream.range(0, depth)
        .mapToObj(i -> table.pathColumnPrefix() + i)
        .toArray(String[]::new);
  }

  private static String[] allColumns(DocumentProperties properties) {
    DocumentTableProperties table = properties.table();
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
