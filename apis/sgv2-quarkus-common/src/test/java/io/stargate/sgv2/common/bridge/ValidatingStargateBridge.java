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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.common.bridge;

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
    return expectation.execute(query.getParameters());
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
              return expectation.execute(batch.getParameters(), batch.getType());
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
                        "Unexpected query, should have been mocked with withQuery(): %s, Values: %s",
                        cql, values)));
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
    private QueryOuterClass.Consistency consistency = QueryOuterClass.Consistency.LOCAL_QUORUM;
    private List<List<QueryOuterClass.Value>> rows;
    private Iterable<? extends QueryOuterClass.ColumnSpec> columnSpec;
    private Function<List<QueryOuterClass.Value>, ByteBuffer> comparableKey;
    private Throwable failure;

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

    public QueryExpectation withConsistency(QueryOuterClass.Consistency consistency) {
      this.consistency = consistency;
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

    public QueryAssert returningFailure(Throwable failure) {
      this.failure = failure;
      return this;
    }

    private boolean matches(String expectedCql, List<QueryOuterClass.Value> expectedValues) {
      Matcher matcher = cqlPattern.matcher(expectedCql);
      boolean valuesEquals = expectedValues.equals(values);
      boolean cqlMatches = matcher.matches();
      return cqlMatches && valuesEquals;
    }

    private Uni<QueryOuterClass.Response> execute(
        QueryOuterClass.BatchParameters parameters, QueryOuterClass.Batch.Type actualBatchType) {
      QueryOuterClass.Consistency actualConsistency =
          Optional.ofNullable(parameters)
              .filter(QueryOuterClass.BatchParameters::hasConsistency)
              .map(p -> p.getConsistency().getValue())
              .orElse(null);

      return execute(actualBatchType, false, null, actualConsistency, null, null);
    }

    private Uni<QueryOuterClass.Response> execute(QueryOuterClass.QueryParameters parameters) {
      Boolean actualEnriched =
          Optional.ofNullable(parameters)
              .map(QueryOuterClass.QueryParameters::getEnriched)
              .orElse(false);

      QueryOuterClass.ResumeMode actualResumeMode =
          Optional.ofNullable(parameters)
              .filter(QueryOuterClass.QueryParameters::hasResumeMode)
              .map(p -> p.getResumeMode().getValue())
              .orElse(null);

      QueryOuterClass.Consistency actualConsistency =
          Optional.ofNullable(parameters)
              .filter(QueryOuterClass.QueryParameters::hasConsistency)
              .map(p -> p.getConsistency().getValue())
              .orElse(null);

      Integer actualPageSize =
          Optional.ofNullable(parameters)
              .filter(QueryOuterClass.QueryParameters::hasPageSize)
              .map(p -> p.getPageSize().getValue())
              .orElse(null);

      BytesValue actualPagingState =
          Optional.ofNullable(parameters)
              .filter(QueryOuterClass.QueryParameters::hasPagingState)
              .map(QueryOuterClass.QueryParameters::getPagingState)
              .orElse(null);

      return execute(
          null,
          actualEnriched,
          actualResumeMode,
          actualConsistency,
          actualPageSize,
          actualPagingState);
    }

    private Uni<QueryOuterClass.Response> execute(
        QueryOuterClass.Batch.Type actualBatchType,
        Boolean actualEnriched,
        QueryOuterClass.ResumeMode actualResumeMode,
        QueryOuterClass.Consistency actualConsistency,
        Integer actualPageSize,
        BytesValue actualPagingState) {

      // assert batch type
      assertThat(actualBatchType)
          .as("Batch type for query %s", cqlPattern)
          .isEqualTo(this.batchType);

      // assert enriched
      assertThat(actualEnriched)
          .as("Enriched flag for the query %s", cqlPattern)
          .isEqualTo(this.enriched);

      // assert resume mode
      assertThat(actualResumeMode)
          .as("Resume mode for the query %s", cqlPattern)
          .isEqualTo(this.resumeMode);

      // assert consistency
      assertThat(actualConsistency)
          .as(
              "Consistency for the query %s not matching, actual %s, expected %s",
              cqlPattern, actualConsistency, this.consistency)
          .isEqualTo(this.consistency);

      // resolve and assert page size
      int pageSizeUsed;
      if (this.pageSize < Integer.MAX_VALUE) {
        pageSizeUsed = this.pageSize;
        assertThat(actualPageSize)
            .as("Page size of %d expected, but query parameters are null.", pageSizeUsed)
            .isNotNull();
        assertThat(actualPageSize)
            .as("Page size mismatch, expected %d but actual was %d", pageSizeUsed, actualPageSize)
            .isEqualTo(pageSizeUsed);
      } else {
        pageSizeUsed = Optional.ofNullable(actualPageSize).orElse(this.pageSize);
      }

      // resolve the paging state
      Optional<ByteBuffer> pagingState =
          Optional.ofNullable(actualPagingState)
              .flatMap(
                  p -> {
                    ByteBuffer byteBuffer = actualPagingState.getValue().asReadOnlyByteBuffer();
                    return Optional.of(byteBuffer);
                  });
      ValidatingPaginator paginator = ValidatingPaginator.of(pageSizeUsed, pagingState);

      // mark as executed
      executed();

      // if we have a throwable throw
      if (null != failure) {
        return Uni.createFrom().failure(failure);
      }

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
