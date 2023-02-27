/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.web.docsapi.service.query;

import io.stargate.db.schema.Column;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Constants needed in the Documents API. */
public interface DocsApiConstants {

  /** the names of columns in the persistence */
  String KEY_COLUMN_NAME = "key";

  String LEAF_COLUMN_NAME = "leaf";

  String STRING_VALUE_COLUMN_NAME = "text_value";

  String DOUBLE_VALUE_COLUMN_NAME = "dbl_value";

  String BOOLEAN_VALUE_COLUMN_NAME = "bool_value";

  /** Search token that matches all values in a path */
  String GLOB_VALUE = "*";

  /** Search token that matches all array elements in a path */
  String GLOB_ARRAY_VALUE = "[*]";

  /** A UID that is used in older collections to represent the root of the document */
  String ROOT_DOC_MARKER = "DOCROOT-a9fb1f04-0394-4c74-b77b-49b4e0ef7900";

  /** A UID that is used to represent an empty object, {} */
  String EMPTY_OBJECT_MARKER = "EMPTYOBJ-bccbeee1-6173-4120-8492-7d7bafaefb1f";

  /** A UID that is used to represent an empty array, [] */
  String EMPTY_ARRAY_MARKER = "EMPTYARRAY-9df4802a-c135-42d6-8be3-d23d9520a4e7";

  String[] VALUE_COLUMN_NAMES =
      new String[] {
        LEAF_COLUMN_NAME,
        STRING_VALUE_COLUMN_NAME,
        DOUBLE_VALUE_COLUMN_NAME,
        BOOLEAN_VALUE_COLUMN_NAME
      };

  /** Gets the name of the column at path depth p */
  Function<Integer, String> P_COLUMN_NAME = p -> "p" + p;

  /** Gets all the names of the columns, in a specific and important order for parameter binding */
  Function<Integer, String[]> ALL_COLUMNS_NAMES =
      depth -> {
        Stream<String> keyCol = Stream.of(KEY_COLUMN_NAME);
        Stream<String> pColumns = IntStream.range(0, depth).mapToObj(P_COLUMN_NAME::apply);
        Stream<String> fixedColumns =
            Stream.of(
                LEAF_COLUMN_NAME,
                STRING_VALUE_COLUMN_NAME,
                DOUBLE_VALUE_COLUMN_NAME,
                BOOLEAN_VALUE_COLUMN_NAME);
        Stream<String> firstConcat = Stream.concat(keyCol, pColumns);
        return Stream.concat(firstConcat, fixedColumns).toArray(String[]::new);
      };

  /** Gets all the names of just path columns */
  Function<Integer, String[]> ALL_PATH_COLUMNS_NAMES =
      depth -> IntStream.range(0, depth).mapToObj(P_COLUMN_NAME::apply).toArray(String[]::new);

  /**
   * Gets all the types of the columns, given a depth and whether you are treating booleans as
   * numeric
   */
  BiFunction<Integer, Boolean, Column.ColumnType[]> ALL_COLUMNS_TYPES =
      (depth, numericBools) -> {
        Stream<Column.ColumnType> keyCol = Stream.of(Column.Type.Text);
        Stream<Column.ColumnType> pColumns =
            IntStream.range(0, depth).mapToObj(i -> Column.Type.Text);
        Stream<Column.ColumnType> fixedColumns =
            Stream.of(
                Column.Type.Text,
                Column.Type.Text,
                Column.Type.Double,
                numericBools ? Column.Type.Tinyint : Column.Type.Boolean);
        Stream<Column.ColumnType> firstConcat = Stream.concat(keyCol, pColumns);
        return Stream.concat(firstConcat, fixedColumns).toArray(Column.ColumnType[]::new);
      };

  /** Gets all the types of the path columns */
  Function<Integer, Column.ColumnType[]> ALL_PATH_COLUMNS_TYPES =
      depth ->
          IntStream.range(0, depth)
              .mapToObj(i -> Column.Type.Text)
              .toArray(Column.ColumnType[]::new);

  /** Gets all columns */
  BiFunction<Integer, Boolean, Column[]> ALL_COLUMNS =
      (depth, numericBools) -> {
        String[] colNames = ALL_COLUMNS_NAMES.apply(depth);
        Column.ColumnType[] colTypes = ALL_COLUMNS_TYPES.apply(depth, numericBools);
        return IntStream.range(0, depth + 5)
            .mapToObj(i -> Column.create(colNames[i], colTypes[i]))
            .toArray(Column[]::new);
      };
}
