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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryFetcherAggregationsTest extends DmlTestBase {

  @Override
  protected List<CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.IOT);
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlQuery, String expectedCqlQuery, List<Value> expectedValues) {
    assertQuery(String.format("query { %s }", graphQlQuery), expectedCqlQuery, expectedValues);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "readings(value: {id: 1}, groupBy: {year: true}) { "
              + "values { _float_function(name: \"sum\", args: [\"value\"]) } }",
          "SELECT SUM(value) FROM iot.readings WHERE id = ? GROUP BY year",
          ImmutableList.of(Values.of(1))),
      arguments(
          "readings(value: {id: 1}, groupBy: {month: true, year: true}) { "
              + "values { _float_function(name: \"sum\", args: [\"value\"]) } }",
          "SELECT SUM(value) FROM iot.readings WHERE id = ? GROUP BY year, month",
          ImmutableList.of(Values.of(1))),
    };
  }
}
