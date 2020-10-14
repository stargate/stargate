package io.stargate.graphql.schema.fetchers.dml;

import static org.assertj.core.api.Assertions.assertThat;

import graphql.ExecutionResult;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.DmlTestBase;
import io.stargate.graphql.schema.SampleKeyspaces;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class MutationFetcherTest extends DmlTestBase {
  @Override
  public Keyspace getKeyspace() {
    return SampleKeyspaces.LIBRARY;
  }

  @Test
  @DisplayName("Atomic mutations with single selection should use normal execution")
  public void mutationAtomicSingleSelectionTest() {
    assertSuccess(
        "mutation @atomic { m1: insertBooks(value: { title: \"a\" } ) { applied } }",
        "INSERT INTO library.books (title) VALUES ('a')");
  }

  @Test
  @DisplayName("Atomic mutations with multiple selections should use batch execution")
  public void mutationAtomicMultipleSelectionTest() {
    ExecutionResult result =
        executeGraphQl(
            "mutation @atomic { "
                + "m1: updateBooks(value: { title: \"1984\", author: \"G.O.\" } ) { applied },"
                + "m2: insertAuthors(value: { author: \"G.O.\", title: \"1984\" } ) { applied },"
                + "m3: deleteBooks(value: { title: \"Animal Farm\" } ) { applied }"
                + "}");
    assertThat(result.getErrors()).isEmpty();
    String[] queries = {
      "UPDATE library.books SET author='G.O.' WHERE title='1984'",
      "INSERT INTO library.authors (author,title) VALUES ('G.O.','1984')",
      "DELETE FROM library.books WHERE title='Animal Farm'"
    };

    assertThat(batchCaptor.getValue()).containsExactly(queries);
  }

  @Test
  @DisplayName("Atomic mutations should use batch options")
  public void mutationAtomicMultipleSelectionWithOptionsTest() {
    ExecutionResult result =
        executeGraphQl(
            "mutation @atomic { "
                + "m1: insertBooks("
                + "  value: { title: \"1984\", author: \"G.O.\" },"
                + "  options: { consistency: ALL }) { applied },"
                + "m2: insertAuthors(value: { author: \"G.O.\", title: \"1984\" } ) { applied }"
                + "}");
    assertThat(result.getErrors()).isEmpty();
    String[] queries = {
      "INSERT INTO library.books (title,author) VALUES ('1984','G.O.')",
      "INSERT INTO library.authors (author,title) VALUES ('G.O.','1984')"
    };

    assertThat(batchCaptor.getValue()).containsExactly(queries);
  }
}
