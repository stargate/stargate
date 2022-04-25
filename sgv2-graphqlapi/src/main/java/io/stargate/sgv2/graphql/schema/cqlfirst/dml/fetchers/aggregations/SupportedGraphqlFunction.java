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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.aggregations;

import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.sgv2.common.grpc.proto.Rows;
import java.util.List;
import java.util.Optional;

public enum SupportedGraphqlFunction {
  INT_FUNCTION("_int_function", Rows::getInt),
  DOUBLE_FUNCTION("_double_function", Rows::getDouble), // corresponds to GraphQLFloat
  BIGINT_FUNCTION("_bigint_function", Rows::getBigint),
  DECIMAL_FUNCTION("_decimal_function", Rows::getDecimal),
  VARINT_FUNCTION("_varint_function", Rows::getVarint),
  FLOAT_FUNCTION("_float_function", Rows::getFloat),
  SMALLINT_FUNCTION("_smallint_function", Rows::getSmallint),
  TINYINT_FUNCTION("_tinyint_function", Rows::getTinyint);

  private final String name;
  private final ValueExtractor rowValueExtractor;

  SupportedGraphqlFunction(String name, ValueExtractor rowValueExtractor) {
    this.name = name;
    this.rowValueExtractor = rowValueExtractor;
  }

  public String getName() {
    return name;
  }

  public ValueExtractor getRowValueExtractor() {
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

  interface ValueExtractor {
    Object extract(Row row, String name, List<ColumnSpec> columns);
  }
}
