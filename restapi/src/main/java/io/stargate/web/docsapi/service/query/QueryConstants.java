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

import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Constants needed in the query. */
public interface QueryConstants {

  String KEY_COLUMN_NAME = "key";

  String LEAF_COLUMN_NAME = "leaf";

  String STRING_VALUE_COLUMN_NAME = "text_value";

  String DOUBLE_VALUE_COLUMN_NAME = "dbl_value";

  String BOOLEAN_VALUE_COLUMN_NAME = "bool_value";

  Function<Integer, String> P_COLUMN_NAME = p -> "p" + p;

  Function<Integer, String[]> ALL_COLUMNS_NAMES =
      depth -> {
        Stream<String> pColumns = IntStream.range(0, depth).mapToObj(P_COLUMN_NAME::apply);
        Stream<String> fixedColumns =
            Stream.of(
                KEY_COLUMN_NAME,
                LEAF_COLUMN_NAME,
                STRING_VALUE_COLUMN_NAME,
                DOUBLE_VALUE_COLUMN_NAME,
                BOOLEAN_VALUE_COLUMN_NAME);
        return Stream.concat(fixedColumns, pColumns).toArray(String[]::new);
      };
}
