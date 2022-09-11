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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import graphql.ExecutionResult;
import graphql.GraphQLError;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.bridge.proto.QueryOuterClass.BatchParameters;
import io.stargate.bridge.proto.QueryOuterClass.BatchQuery;
import io.stargate.bridge.proto.QueryOuterClass.Consistency;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class BulkMutationFetcherTest extends DmlTestBase {

  @Override
  protected List<Schema.CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.LIBRARY);
  }

  @Test
  @DisplayName("Atomic bulk mutations with single selection should use normal execution")
  public void mutationAtomicBulkSingleSelectionTest() {
    assertQuery(
        "mutation @atomic { m1: bulkInsertbooks(values: [{ title: \"a\" }] ) { applied } }",
        "INSERT INTO library.books (title) VALUES (?)",
        ImmutableList.of(Values.of("a")));
  }

  @Test
  @DisplayName("Atomic bulk mutations with multiple bulk queries should use batch execution")
  public void mutationAtomicBulkMultipleSelectionTest() {
    ExecutionResult result =
        executeGraphql(
            "mutation @atomic { "
                + "m1: bulkInsertbooks(values: [{ title: \"1984\", author: \"G.O.\" }] ) { applied },"
                + "m2: bulkInsertauthors(values: [{ author: \"G.O.\", title: \"1984\" }] ) { applied },"
                + "m3: deletebooks(value: { title: \"Animal Farm\" } ) { applied }"
                + "}");
    assertThat(result.getErrors()).isEmpty();

    Batch batch = getCapturedBatch();
    assertThat(batch.getQueriesCount()).isEqualTo(3);

    BatchQuery query1 = batch.getQueries(0);
    assertThat(query1.getCql())
        .isEqualTo("INSERT INTO library.books (title, author) VALUES (?, ?)");
    assertThat(query1.getValues().getValuesList())
        .containsExactly(Values.of("1984"), Values.of("G.O."));

    BatchQuery query2 = batch.getQueries(1);
    assertThat(query2.getCql())
        .isEqualTo("INSERT INTO library.authors (author, title) VALUES (?, ?)");
    assertThat(query2.getValues().getValuesList())
        .containsExactly(Values.of("G.O."), Values.of("1984"));

    BatchQuery query3 = batch.getQueries(2);
    assertThat(query3.getCql()).isEqualTo("DELETE FROM library.books WHERE title = ?");
    assertThat(query3.getValues().getValuesList()).containsExactly(Values.of("Animal Farm"));
  }

  @ParameterizedTest
  @DisplayName("Atomic bulk mutations should use batch options")
  @ValueSource(strings = {"LOCAL_QUORUM", "ALL"})
  public void mutationAtomicMultipleSelectionWithOptionsTest(String cl) {
    ExecutionResult result =
        executeGraphql(
            String.format(
                "mutation @atomic { "
                    + "m1: bulkInsertbooks("
                    + "  values: [{ title: \"1984\", author: \"G.O.\" }],"
                    + "  options: { consistency: %s }) { applied },"
                    + "m2: bulkInsertauthors(values: [{ author: \"G.O.\", title: \"1984\" }] ) { applied }"
                    + "}",
                cl));
    assertThat(result.getErrors()).isEmpty();

    BatchParameters parameters = getCapturedBatch().getParameters();
    assertThat(parameters.getConsistency().getValue()).isEqualTo(Consistency.valueOf(cl));

    // Test with options in second position
    result =
        executeGraphql(
            String.format(
                "mutation @atomic { "
                    + "m1: bulkInsertbooks(values: [{ title: \"1984\", author: \"G.O.\" }]) { applied },"
                    + "m2: bulkInsertauthors("
                    + "  values: [{ author: \"G.O.\", title: \"1984\" }],"
                    + "  options: { consistency: %s, serialConsistency: LOCAL_SERIAL }) {  applied }"
                    + "}",
                cl));
    assertThat(result.getErrors()).isEmpty();

    parameters = getCapturedBatch().getParameters();
    assertThat(parameters.getConsistency().getValue()).isEqualTo(Consistency.valueOf(cl));
    assertThat(parameters.getSerialConsistency().getValue()).isEqualTo(Consistency.LOCAL_SERIAL);
  }

  @Test
  @DisplayName("Atomic bulk mutations with different batch options should fail")
  public void mutationAtomicMultipleSelectionWithDifferentOptionsFailTest() {
    ExecutionResult result =
        executeGraphql(
            "mutation @atomic { "
                + "m1: bulkInsertbooks("
                + "  values: [{ title: \"1984\", author: \"G.O.\" }],"
                + "  options: { consistency: ALL }) { applied },"
                + "m2: bulkInsertauthors("
                + "  values: [{ author: \"G.O.\", title: \"1984\" }],"
                + "  options: { consistency: LOCAL_ONE }) { applied }"
                + "}");

    assertThat(result.getErrors())
        .hasSize(2)
        .extracting(GraphQLError::getMessage)
        .containsExactly(
            "Exception while fetching data (/m1) : options can only de defined once in an @atomic mutation selection",
            "Exception while fetching data (/m2) : options can only de defined once in an @atomic mutation selection");
  }
}
