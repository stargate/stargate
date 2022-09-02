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
package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.sgv2.graphql.schema.cqlfirst.ddl.DdlTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CreateIndexFetcherTest extends DdlTestBase {
  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected CQL query")
  public void queryTest(String graphQlMutation, String expectedCqlQuery) {
    assertQuery(String.format("mutation { %s }", graphQlMutation), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"title\")",
          "CREATE INDEX ON library.sales (title)"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"title\", indexName:\"test_idx\")",
          "CREATE INDEX test_idx ON library.sales (title)"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"title\", indexName:\"test_idx\", ifNotExists: true)",
          "CREATE INDEX IF NOT EXISTS test_idx ON library.sales (title)"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"title\", indexType:\"org.apache.cassandra.index.sasi.SASIIndex\")",
          "CREATE CUSTOM INDEX ON library.sales (title) USING 'org.apache.cassandra.index.sasi.SASIIndex'"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"ranking\", indexKind: KEYS)",
          "CREATE INDEX ON library.sales (KEYS(ranking))"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"ranking\", indexKind: ENTRIES)",
          "CREATE INDEX ON library.sales (ENTRIES(ranking))"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"ranking\", indexKind: VALUES)",
          "CREATE INDEX ON library.sales (VALUES(ranking))"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\", columnName:\"prices\", indexKind: FULL)",
          "CREATE INDEX ON library.sales (FULL(prices))"),
    };
  }

  @ParameterizedTest
  @MethodSource("failingQueries")
  @DisplayName("Should execute GraphQL and throw expected error")
  public void errorTest(String graphQlMutation, String expectedError) {
    assertError(String.format("mutation { %s }", graphQlMutation), expectedError);
  }

  public static Arguments[] failingQueries() {
    return new Arguments[] {
      arguments(
          "createIndex(tableName:\"sales\", columnName:\"title\")",
          "Missing field argument keyspaceName @ 'createIndex'"),
      arguments(
          "createIndex(keyspaceName:\"library\", columnName:\"title\")",
          "Missing field argument tableName @ 'createIndex'"),
      arguments(
          "createIndex(keyspaceName:\"library\", tableName:\"sales\")",
          "Missing field argument columnName @ 'createIndex'"),
    };
  }
}
