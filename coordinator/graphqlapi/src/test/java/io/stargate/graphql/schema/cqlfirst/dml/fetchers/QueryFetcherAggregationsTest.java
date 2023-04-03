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
package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import graphql.schema.GraphQLSchema;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.SampleKeyspaces;
import io.stargate.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryFetcherAggregationsTest extends DmlTestBase {
  private final GraphQLSchema schema = createGraphQlSchema();

  @Override
  public Schema getCQLSchema() {
    return Schema.create(Collections.singleton(SampleKeyspaces.IOT));
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlQuery, String expectedCqlQuery) {
    assertQuery(String.format("query { %s }", graphQlQuery), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "readings(value: {id: 1}, groupBy: {year: true}) { "
              + "values { _float_function(name: \"sum\", args: [\"value\"]) } }",
          "SELECT SUM(value) FROM iot.readings WHERE id = 1 GROUP BY year"),
      arguments(
          "readings(value: {id: 1}, groupBy: {month: true, year: true}) { "
              + "values { _float_function(name: \"sum\", args: [\"value\"]) } }",
          "SELECT SUM(value) FROM iot.readings WHERE id = 1 GROUP BY year, month"),
    };
  }
}
