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
package io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations;

import io.stargate.db.datastore.Row;
import java.util.Optional;
import java.util.function.BiFunction;

public enum SupportedGraphqlFunction {
  INT_FUNCTION("_int_function", Row::getInt),
  DOUBLE_FUNCTION("_double_function", Row::getDouble), // corresponds to GraphQLFloat
  BIGINT_FUNCTION("_bigint_function", Row::getLong),
  DECIMAL_FUNCTION("_decimal_function", Row::getBigDecimal),
  VARINT_FUNCTION("_varint_function", Row::getBigInteger),
  FLOAT_FUNCTION("_float_function", Row::getFloat),
  SMALLINT_FUNCTION("_smallint_function", Row::getShort),
  TINYINT_FUNCTION("_tinyint_function", Row::getByte);

  private final String name;
  private final BiFunction<Row, String, Object> rowValueExtractor;

  SupportedGraphqlFunction(String name, BiFunction<Row, String, Object> rowValueExtractor) {
    this.name = name;
    this.rowValueExtractor = rowValueExtractor;
  }

  public String getName() {
    return name;
  }

  public BiFunction<Row, String, Object> getRowValueExtractor() {
    return rowValueExtractor;
  }

  public static Optional<SupportedGraphqlFunction> valueOfIgnoreCase(String functionName) {
    for (SupportedGraphqlFunction f : values()) {
      if (f.name.equalsIgnoreCase(functionName)) {
        return Optional.of(f);
      }
    }
    return Optional.empty();
  }
}
