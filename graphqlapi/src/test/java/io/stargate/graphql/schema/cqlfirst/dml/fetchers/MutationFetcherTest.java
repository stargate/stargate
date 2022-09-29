package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static org.assertj.core.api.Assertions.assertThat;

import graphql.ExecutionResult;
import graphql.GraphQLError;
import io.stargate.db.Parameters;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.SampleKeyspaces;
import io.stargate.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.Collections;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class MutationFetcherTest extends DmlTestBase {
  @Override
  public Schema getCQLSchema() {
    return Schema.create(Collections.singleton(SampleKeyspaces.LIBRARY));
  }

  @Test
  @DisplayName("Atomic mutations with single selection should use normal execution")
  public void mutationAtomicSingleSelectionTest() {
    assertQuery(
        "mutation @atomic { m1: insertbooks(value: { title: \"a\" } ) { applied } }",
        "INSERT INTO library.books (title) VALUES ('a')");
  }

  @Test
  @DisplayName("Atomic mutations with multiple selections should use batch execution")
  public void mutationAtomicMultipleSelectionTest() {
    ExecutionResult result =
        executeGraphQl(
            "mutation @atomic { "
                + "m1: updatebooks(value: { title: \"1984\", author: \"G.O.\" } ) { applied },"
                + "m2: insertauthors(value: { author: \"G.O.\", title: \"1984\" } ) { applied },"
                + "m3: deletebooks(value: { title: \"Animal Farm\" } ) { applied }"
                + "}");
    assertThat(result.getErrors()).isEmpty();
    String[] queries = {
      "UPDATE library.books SET author = 'G.O.' WHERE title = '1984'",
      "INSERT INTO library.authors (author, title) VALUES ('G.O.', '1984')",
      "DELETE FROM library.books WHERE title = 'Animal Farm'"
    };

    assertThat(getCapturedBatchQueriesString()).containsExactly(queries);
  }

  @ParameterizedTest
  @DisplayName("Atomic mutations should use batch options")
  @ValueSource(strings = {"LOCAL_QUORUM", "ALL"})
  public void mutationAtomicMultipleSelectionWithOptionsTest(String cl) {
    ExecutionResult result =
        executeGraphQl(
            String.format(
                "mutation @atomic { "
                    + "m1: insertbooks("
                    + "  value: { title: \"1984\", author: \"G.O.\" },"
                    + "  options: { consistency: %s }) { applied },"
                    + "m2: insertauthors(value: { author: \"G.O.\", title: \"1984\" } ) { applied }"
                    + "}",
                cl));
    assertThat(result.getErrors()).isEmpty();
    String[] queries = {
      "INSERT INTO library.books (title, author) VALUES ('1984', 'G.O.')",
      "INSERT INTO library.authors (author, title) VALUES ('G.O.', '1984')"
    };

    assertThat(getCapturedBatchQueriesString()).containsExactly(queries);
    assertThat(getCapturedParameters())
        .extracting(Parameters::consistencyLevel)
        .isEqualTo(ConsistencyLevel.valueOf(cl));

    // Test with options in second position
    result =
        executeGraphQl(
            String.format(
                "mutation @atomic { "
                    + "m1: insertbooks(value: { title: \"1984\", author: \"G.O.\" }) { applied },"
                    + "m2: insertauthors("
                    + "  value: { author: \"G.O.\", title: \"1984\" },"
                    + "  options: { consistency: %s, serialConsistency: LOCAL_SERIAL }) { applied }"
                    + "}",
                cl));
    assertThat(result.getErrors()).isEmpty();
    assertThat(getCapturedBatchQueriesString()).containsExactly(queries);
    assertThat(getCapturedParameters())
        .extracting(Parameters::consistencyLevel, p -> p.serialConsistencyLevel().get())
        .containsExactly(ConsistencyLevel.valueOf(cl), ConsistencyLevel.LOCAL_SERIAL);
  }

  @Test
  @DisplayName("Atomic mutations with different batch options should fail")
  public void mutationAtomicMultipleSelectionWithDifferentOptionsFailTest() {
    ExecutionResult result =
        executeGraphQl(
            "mutation @atomic { "
                + "m1: insertbooks("
                + "  value: { title: \"1984\", author: \"G.O.\" },"
                + "  options: { consistency: ALL }) { applied },"
                + "m2: insertauthors("
                + "  value: { author: \"G.O.\", title: \"1984\" },"
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
