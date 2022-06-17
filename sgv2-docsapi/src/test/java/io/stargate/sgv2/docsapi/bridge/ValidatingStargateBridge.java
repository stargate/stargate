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
package io.stargate.sgv2.docsapi.bridge;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.assertj.core.api.AbstractIntegerAssert;

/**
 * A mock bridge implementation for unit tests.
 *
 * @author Olivier Michallat
 * @author Dmitri Bourlatchkov
 * @author Ivan Senic
 */
public class ValidatingStargateBridge implements StargateBridge {

  private final List<QueryExpectation> expectedQueries = new ArrayList<>();

  public void reset() {
    expectedQueries.clear();
  }

  public void validate() {
    expectedQueries.forEach(QueryExpectation::validate);
  }

  @Override
  public Uni<QueryOuterClass.Response> executeQuery(QueryOuterClass.Query query) {
    QueryExpectation expectation =
        findQueryExpectation(query.getCql(), query.getValues().getValuesList());
    return expectation.execute(query.getParameters(), null);
  }

  @Override
  public Uni<Schema.QueryWithSchemaResponse> executeQueryWithSchema(
      Schema.QueryWithSchema request) {
    // Simulate a successful execution (keyspace was up to date)
    return executeQuery(request.getQuery())
        .map(response -> Schema.QueryWithSchemaResponse.newBuilder().setResponse(response).build());
  }

  @Override
  public Uni<QueryOuterClass.Response> executeBatch(QueryOuterClass.Batch batch) {
    return batch.getQueriesList().stream()
        .map(
            query -> {
              QueryExpectation expectation =
                  findQueryExpectation(query.getCql(), query.getValues().getValuesList());
              return expectation.execute(null, batch.getType());
            })
        // Return the last result
        .reduce((first, second) -> second)
        .orElseThrow(() -> new AssertionError("Batch should have at least one query"));
  }

  private QueryExpectation findQueryExpectation(String cql, List<QueryOuterClass.Value> values) {
    return expectedQueries.stream()
        .filter(q -> q.matches(cql, values))
        .findFirst()
        .orElseThrow(
            () ->
                new AssertionError(
                    String.format(
                        "Unexpected query, should have been mocked with withQuery(): %s", cql)));
  }

  @Override
  public Uni<Schema.CqlKeyspaceDescribe> describeKeyspace(Schema.DescribeKeyspaceQuery request) {
    throw new UnsupportedOperationException("Not implemented by this mock");
  }

  @Override
  public Uni<Schema.AuthorizeSchemaReadsResponse> authorizeSchemaReads(
      Schema.AuthorizeSchemaReadsRequest request) {
    throw new UnsupportedOperationException("Not implemented by this mock");
  }

  @Override
  public Uni<Schema.SupportedFeaturesResponse> getSupportedFeatures(
      Schema.SupportedFeaturesRequest request) {
    throw new UnsupportedOperationException("Not implemented by this mock");
  }

  private QueryExpectation add(QueryExpectation expectation) {
    expectedQueries.add(expectation);
    return expectation;
  }

  public QueryExpectation withQuery(String cql, QueryOuterClass.Value... values) {
    return add(new QueryExpectation(Pattern.quote(cql), Arrays.asList(values)));
  }

  public QueryExpectation withAnySelectFrom(String keyspace, String table) {
    String regex =
        """
        SELECT.*FROM.*\\"%s\\"\\.\\"%s\\".*
        """.formatted(keyspace, table);

    return add(new QueryExpectation(regex, Collections.emptyList()));
  }

  public abstract static class QueryAssert {

    private final AtomicInteger executeCount = new AtomicInteger();

    public AbstractIntegerAssert<?> assertExecuteCount() {
      return assertThat(executeCount.get());
    }

    void executed() {
      executeCount.incrementAndGet();
    }
  }

  public static class QueryExpectation extends QueryAssert {

    private final Pattern cqlPattern;
    private final List<QueryOuterClass.Value> values;
    private int pageSize = Integer.MAX_VALUE;
    private QueryOuterClass.Batch.Type batchType;
    private boolean enriched;
    private QueryOuterClass.ResumeMode resumeMode;
    private List<List<QueryOuterClass.Value>> rows;
    private Iterable<? extends QueryOuterClass.ColumnSpec> columnSpec;

    private Function<List<QueryOuterClass.Value>, ByteBuffer> comparableKey;

    private QueryExpectation(String cqlRegex, List<QueryOuterClass.Value> values) {
      this.cqlPattern = Pattern.compile(cqlRegex);
      this.values = values;
    }

    private QueryExpectation(String cqlRegex) {
      this(cqlRegex, Collections.emptyList());
    }

    public QueryExpectation withPageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public QueryExpectation inBatch(QueryOuterClass.Batch.Type batchType) {
      this.batchType = batchType;
      return this;
    }

    public QueryExpectation enriched() {
      enriched = true;
      return this;
    }

    public QueryExpectation withResumeMode(QueryOuterClass.ResumeMode resumeMode) {
      this.resumeMode = resumeMode;
      return this;
    }

    public QueryExpectation withColumnSpec(
        Iterable<? extends QueryOuterClass.ColumnSpec> columnSpec) {
      this.columnSpec = columnSpec;
      return this;
    }

    public QueryExpectation withComparableKey(
        Function<List<QueryOuterClass.Value>, ByteBuffer> comparableKey) {
      this.comparableKey = comparableKey;
      return this;
    }

    public QueryAssert returningNothing() {

      return returning(Collections.emptyList());
    }

    public QueryAssert returning(List<List<QueryOuterClass.Value>> rows) {
      this.rows = rows;
      return this;
    }

    private boolean matches(String expectedCql, List<QueryOuterClass.Value> expectedValues) {
      Matcher matcher = cqlPattern.matcher(expectedCql);
      boolean valuesEquals = expectedValues.equals(values);
      boolean cqlMatches = matcher.matches();
      return cqlMatches && valuesEquals;
    }

    private Uni<QueryOuterClass.Response> execute(
        QueryOuterClass.QueryParameters parameters, QueryOuterClass.Batch.Type actualBatchType) {

      // assert batch type
      assertThat(this.batchType)
          .as("Batch type for query %s", cqlPattern)
          .isEqualTo(actualBatchType);

      // assert enriched
      Boolean actualEnriched =
          Optional.ofNullable(parameters)
              .map(QueryOuterClass.QueryParameters::getEnriched)
              .orElse(false);
      assertThat(this.enriched)
          .as("Enriched flag for the query %s", cqlPattern)
          .isEqualTo(actualEnriched);

      // assert resume mode
      QueryOuterClass.ResumeMode actualResumeMode =
          Optional.ofNullable(parameters)
              .filter(QueryOuterClass.QueryParameters::hasResumeMode)
              .map(p -> p.getResumeMode().getValue())
              .orElse(null);
      assertThat(this.resumeMode)
          .as("Resume mode for the query %s", cqlPattern)
          .isEqualTo(actualResumeMode);

      // resolve and assert page size
      int pageSize;
      if (this.pageSize < Integer.MAX_VALUE) {
        pageSize = this.pageSize;
        int actual = parameters.getPageSize().getValue();
        assertThat(parameters)
            .isNotNull()
            .withFailMessage(
                "Page size of %d expected, but query parameters are null.".formatted(pageSize));
        assertThat(actual)
            .isEqualTo(pageSize)
            .withFailMessage(
                "Page size mismatch, expected %d but actual was %d".formatted(pageSize, actual));
      } else {
        pageSize =
            Optional.ofNullable(parameters)
                .map(p -> p.getPageSize().getValue())
                .orElse(this.pageSize);
      }

      // resolve the paging state
      Optional<ByteBuffer> pagingState =
          Optional.ofNullable(parameters)
              .flatMap(
                  p -> {
                    if (p.hasPagingState()) {
                      ByteBuffer byteBuffer = p.getPagingState().getValue().asReadOnlyByteBuffer();
                      return Optional.of(byteBuffer);
                    } else {
                      return Optional.empty();
                    }
                  });
      ValidatingPaginator paginator = ValidatingPaginator.of(pageSize, pagingState);

      // mark as executed
      executed();

      QueryOuterClass.ResultSet.Builder resultSet = QueryOuterClass.ResultSet.newBuilder();

      // filter rows in order to respect the page size
      // and get next paging state
      List<List<QueryOuterClass.Value>> filterRows = paginator.filter(rows);
      ByteBuffer nextPagingState = paginator.pagingState();
      if (null != nextPagingState) {
        resultSet.setPagingState(
            BytesValue.newBuilder().setValue(ByteString.copyFrom(nextPagingState)).build());
      }
      // for each filtered row
      for (int i = 0; i < filterRows.size(); i++) {
        // figure out the row and add all items
        List<QueryOuterClass.Value> row = filterRows.get(i);
        QueryOuterClass.Row.Builder builder = QueryOuterClass.Row.newBuilder().addAllValues(row);

        // make the page state for a row
        ByteBuffer rowPageState = paginator.pagingStateForRow(i);
        if (null != rowPageState) {
          builder.setPagingState(
              BytesValue.newBuilder().setValue(ByteString.copyFrom(rowPageState)).build());
        }

        if (null != comparableKey) {
          ByteString value = ByteString.copyFrom(comparableKey.apply(row));
          builder.setComparableBytes(BytesValue.newBuilder().setValue(value));
        }

        resultSet.addRows(builder);
      }

      // if columns spec was defined, pass
      if (null != columnSpec) {
        resultSet.addAllColumns(columnSpec);
      }

      return Uni.createFrom()
          .item(QueryOuterClass.Response.newBuilder().setResultSet(resultSet).build());
    }

    private void validate() {
      assertExecuteCount()
          .withFailMessage(
              "No queries were executed for this expected pattern: %s, values: %s",
              cqlPattern, values)
          .isGreaterThanOrEqualTo(1);
    }
  }
}
