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
package io.stargate.sgv2.graphql.integration.graphqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.grpc.Values;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.util.List;
import java.util.Map;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AtomicIntegrationTest extends GraphqlFirstIntegrationTest {

  @BeforeAll
  public void deploySchema() {
    client.deploySchema(
        keyspaceName,
        "type Foo @cql_input {\n"
            + "  k: Int @cql_column(partitionKey: true)\n"
            + "  cc: Int @cql_column(clusteringOrder: ASC)\n"
            + "  v: Int\n"
            + "}\n"
            + "type InsertFooResponse @cql_payload {\n"
            + "  foo: Foo\n"
            + "  applied: Boolean\n"
            + "}\n"
            + "type Query {\n"
            + "  foo(k: Int, cc: Int): Foo\n"
            + "}\n"
            + "type Mutation {\n"
            + "  insertFoo(foo: FooInput): InsertFooResponse\n"
            + "  insertFooLocalOne(foo: FooInput): InsertFooResponse\n"
            + "      @cql_insert(consistencyLevel: LOCAL_ONE)\n"
            + "  insertFooIfNotExists(foo: FooInput): InsertFooResponse\n"
            + "  insertFoos(foos: [FooInput]): [InsertFooResponse]\n"
            + "  insertFoosIfNotExists(foos: [FooInput]): [InsertFooResponse]\n"
            + "}\n");
  }

  @BeforeEach
  public void cleanupData() {
    executeCql("truncate table \"Foo\"");
  }

  @Test
  @DisplayName("Should batch simple operations")
  public void simpleOperations() {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  insert1: insertFoo(foo: { k: 1, cc: 1, v: 1 }) { applied }\n"
            + "  insert2: insertFoo(foo: { k: 1, cc: 2, v: 2 }) { applied }\n"
            + "}\n";

    // When
    Object response = client.executeKeyspaceQuery(keyspaceName, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.insert1.applied")).isTrue();
    assertThat(JsonPath.<Boolean>read(response, "$.insert2.applied")).isTrue();

    long writeTime1 = getWriteTime(1, 1);
    long writeTime2 = getWriteTime(1, 2);
    assertThat(writeTime1).isEqualTo(writeTime2);
  }

  @Test
  @DisplayName("Should batch bulk insert")
  public void bulkInsert() {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  insertFoos(foos: [\n"
            + "    { k: 1, cc: 1, v: 1 }, "
            + "    { k: 1, cc: 2, v: 2 } "
            + "  ]) { applied }\n"
            + "}\n";

    // When
    Object response = client.executeKeyspaceQuery(keyspaceName, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.insertFoos[0].applied")).isTrue();
    assertThat(JsonPath.<Boolean>read(response, "$.insertFoos[1].applied")).isTrue();

    long writeTime1 = getWriteTime(1, 1);
    long writeTime2 = getWriteTime(1, 2);
    assertThat(writeTime1).isEqualTo(writeTime2);
  }

  @Test
  @DisplayName("Should batch mix of simple and insert operations")
  public void simpleAndBulkOperations() {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  insertFoos(foos: [\n"
            + "    { k: 1, cc: 1, v: 1 }, "
            + "    { k: 1, cc: 2, v: 2 } "
            + "  ]) { applied }\n"
            + "  insertFoo(foo: { k: 1, cc: 3, v: 3 }) { applied }\n"
            + "}\n";

    // When
    Object response = client.executeKeyspaceQuery(keyspaceName, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.insertFoos[0].applied")).isTrue();
    assertThat(JsonPath.<Boolean>read(response, "$.insertFoos[1].applied")).isTrue();
    assertThat(JsonPath.<Boolean>read(response, "$.insertFoo.applied")).isTrue();

    long writeTime1 = getWriteTime(1, 1);
    long writeTime2 = getWriteTime(1, 2);
    long writeTime3 = getWriteTime(1, 3);
    assertThat(writeTime1).isEqualTo(writeTime2).isEqualTo(writeTime3);
  }

  @Test
  @DisplayName("Should handle successful conditional batch")
  public void successfulConditionalBatch() {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  insert1: insertFooIfNotExists(foo: { k: 1, cc: 1, v: 1 }) {\n"
            + "    applied, foo { k cc v }\n"
            + "  }\n"
            // Not all queries have to be LWTs, a conditional batch can also contain regular ones:
            + "  insert2: insertFoo(foo: { k: 1, cc: 2, v: 2 }) {\n"
            + "    applied, foo { k cc v }\n"
            + "  }\n"
            + "}\n";

    // When
    Object response = client.executeKeyspaceQuery(keyspaceName, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.insert1.applied")).isTrue();
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.v")).isEqualTo(1);

    assertThat(JsonPath.<Boolean>read(response, "$.insert2.applied")).isTrue();
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.cc")).isEqualTo(2);
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.v")).isEqualTo(2);

    long writeTime1 = getWriteTime(1, 1);
    long writeTime2 = getWriteTime(1, 2);
    assertThat(writeTime1).isEqualTo(writeTime2);
  }

  @Test
  @DisplayName("Should handle failed conditional batch with non-LWT queries")
  public void failedConditionalBatch() {
    // Given
    executeCql("INSERT INTO \"Foo\" (k, cc, v) VALUES (1, 1, 2)");
    String query =
        "mutation @atomic {\n"
            + "  insert1: insertFooIfNotExists(foo: { k: 1, cc: 1, v: 1 }) {\n"
            + "    applied, foo { k cc v }\n"
            + "  }\n"
            + "  insert2: insertFoo(foo: { k: 1, cc: 2, v: 2 }) {\n"
            + "    applied, foo { k cc v }\n"
            + "  }\n"
            + "}\n";

    // When
    Object response = client.executeKeyspaceQuery(keyspaceName, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.insert1.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.v")).isEqualTo(2);

    // For non-LWT queries, we don't have any data to echo back because the batch response does not
    // contain that row
    assertThat(JsonPath.<Boolean>read(response, "$.insert2.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.k")).isNull();
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.cc")).isNull();
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.v")).isNull();
  }

  @Test
  @DisplayName("Should handle failed conditional batch when queries are not in PK order")
  public void failedConditionalBatchOutOfOrder() {
    // Given
    executeCql("INSERT INTO \"Foo\" (k, cc, v) VALUES (1, 1, 2)");
    executeCql("INSERT INTO \"Foo\" (k, cc, v) VALUES (1, 2, 3)");
    String query =
        "mutation @atomic {\n"
            + "  insert2: insertFooIfNotExists(foo: { k: 1, cc: 2, v: 2 }) {\n"
            + "    applied, foo { k cc v }\n"
            + "  }\n"
            + "  insert1: insertFooIfNotExists(foo: { k: 1, cc: 1, v: 1 }) {\n"
            + "    applied, foo { k cc v }\n"
            + "  }\n"
            + "}\n";

    // When
    Object response = client.executeKeyspaceQuery(keyspaceName, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.insert2.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.cc")).isEqualTo(2);
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.v")).isEqualTo(3);

    assertThat(JsonPath.<Boolean>read(response, "$.insert1.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.v")).isEqualTo(2);
  }

  @Test
  @DisplayName(
      "Should handle failed conditional batch when multiple queries operate on the same PK")
  public void failedConditionalDuplicatePks() {
    // Given
    executeCql("INSERT INTO \"Foo\" (k, cc, v) VALUES (1, 1, 2)");
    String query =
        "mutation @atomic {\n"
            + "  insert1: insertFooIfNotExists(foo: { k: 1, cc: 1, v: 1 }) {\n"
            + "    applied, foo { k cc v }\n"
            + "  }\n"
            + "  insert2: insertFooIfNotExists(foo: { k: 1, cc: 1, v: 3 }) {\n"
            + "    applied, foo { k cc v }\n"
            + "  }\n"
            + "}\n";

    // When
    Object response = client.executeKeyspaceQuery(keyspaceName, query);

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.insert1.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert1.foo.v")).isEqualTo(2);

    assertThat(JsonPath.<Boolean>read(response, "$.insert2.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.k")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.cc")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.insert2.foo.v")).isEqualTo(2);
  }

  @Test
  @DisplayName("Should fail when operations don't use the same query parameters")
  public void failOnDifferentQueryParameters() {
    // Given
    String query =
        "mutation @atomic {\n"
            + "  insert1: insertFoo(foo: { k: 1, cc: 1, v: 1 }) { applied }\n"
            + "  insert2: insertFooLocalOne(foo: { k: 1, cc: 2, v: 2 }) { applied }\n"
            + "  insert3: insertFoo(foo: { k: 1, cc: 3, v: 3 }) { applied }\n"
            + "}\n";

    // When
    List<Map<String, Object>> errors = client.getKeyspaceErrors(keyspaceName, query);

    // Then
    assertThat(errors).hasSize(3);

    Map<String, Object> error1 = errors.get(0);
    assertThat(JsonPath.<List<String>>read(error1, "$.path")).containsOnly("insert1");
    assertThat(JsonPath.<String>read(error1, "$.message"))
        .contains(
            "@atomic mutation aborted because one of the operations failed (see other errors for details)");
    Map<String, Object> error2 = errors.get(1);

    assertThat(JsonPath.<List<String>>read(error2, "$.path")).containsOnly("insert2");
    assertThat(JsonPath.<String>read(error2, "$.message"))
        .contains("all the selections in an @atomic mutation must use the same consistency levels");

    Map<String, Object> error3 = errors.get(2);
    assertThat(JsonPath.<List<String>>read(error3, "$.path")).containsOnly("insert3");
    assertThat(JsonPath.<String>read(error3, "$.message"))
        .contains(
            "@atomic mutation aborted because one of the operations failed (see other errors for details)");
  }

  private long getWriteTime(int k, int cc) {
    String cql = "SELECT writetime(v) FROM \"Foo\" WHERE k = %d AND cc = %d".formatted(k, cc);
    return Values.bigint(executeCql(cql).getResultSet().getRows(0).getValues(0));
  }
}
