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
package io.stargate.sgv2.graphql.integration.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.CqlFirstIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class InvalidQueriesIntegrationTest extends CqlFirstIntegrationTest {

  @BeforeAll
  public void createSchema() {
    session.execute("CREATE TABLE \"Foo\"(k int PRIMARY KEY, v int)");
  }

  @ParameterizedTest
  @MethodSource("dmlErrors")
  @DisplayName("Should return expected error for bad DML query")
  public void ddlQuery(String query, String expectedError) {
    assertThat(client.getDmlQueryError(keyspaceId.asInternal(), query)).contains(expectedError);
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  public static Arguments[] dmlErrors() {
    return new Arguments[] {
      arguments(
          "query { zzz { name } }",
          "Validation error of type FieldUndefined: Field 'zzz' in type 'Query' is undefined @ 'zzz'"),
      arguments(
          "invalidWrapper { zzz { name } }", "Invalid Syntax : offending token 'invalidWrapper'"),
      arguments(
          "query { Foo(filter: { v: { gt: 1} }) { values { k v } }}",
          "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance"),
    };
  }

  @ParameterizedTest
  @MethodSource("ddlErrors")
  @DisplayName("Should return expected error for bad DDL query")
  public void dmlQuery(String query, String expectedError) {
    assertThat(client.getDdlQueryError(query)).contains(expectedError);
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  public static Arguments[] ddlErrors() {
    return new Arguments[] {
      arguments(
          "query { zzz { name } }",
          "Validation error of type FieldUndefined: Field 'zzz' in type 'Query' is undefined"),
      arguments(
          "query { keyspace (name: 1) { name } }",
          "Validation error of type WrongType: argument 'name' with value 'IntValue{value=1}' is not a valid 'String'"),
      arguments(
          "query { keyspaces { name, nameInvalid } }",
          "Validation error of type FieldUndefined: Field 'nameInvalid' in type 'Keyspace' is undefined"),
    };
  }
}
