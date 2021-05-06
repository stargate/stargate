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

public enum SupportedGraphqlFunction {
  INT_FUNCTION("_int_function"),
  DOUBLE_FUNCTION("_double_function"), // corresponds to GraphQLFloat
  BIGINT_FUNCTION("_bigint_function"),
  DECIMAL_FUNCTION("_decimal_function"),
  VARINT_FUNCTION("_varint_function"),
  FLOAT_FUNCTION("_float_function"),
  SMALLINT_FUNCTION("_smallint_function"),
  TINYINT_FUNCTION("_tinyint_function");

  private final String name;

  SupportedGraphqlFunction(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static SupportedGraphqlFunction valueOfIgnoreCase(String functionName) {
    for (SupportedGraphqlFunction f : values()) {
      if (f.name.equalsIgnoreCase(functionName)) {
        return f;
      }
    }
    throw new UnsupportedOperationException(
        String.format("The graphQL aggregation function: %s is not supported.", functionName));
  }
}
