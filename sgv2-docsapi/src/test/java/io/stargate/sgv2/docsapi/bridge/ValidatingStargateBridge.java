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

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.assertj.core.api.AbstractIntegerAssert;

/** A mock bridge implementation for unit tests. */
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
    return expectation.execute(null, query.hasParameters() && query.getParameters().getEnriched());
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
              return expectation.execute(batch.getType(), false);
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
    private QueryOuterClass.Batch.Type batchType;
    private boolean enriched;
    private List<List<QueryOuterClass.Value>> rows;

    private QueryExpectation(String cqlRegex, List<QueryOuterClass.Value> values) {
      this.cqlPattern = Pattern.compile(cqlRegex);
      this.values = values;
    }

    private QueryExpectation(String cqlRegex) {
      this(cqlRegex, Collections.emptyList());
    }

    public QueryExpectation inBatch(QueryOuterClass.Batch.Type batchType) {
      this.batchType = batchType;
      return this;
    }

    public QueryExpectation enriched() {
      enriched = true;
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
      return cqlPattern.matcher(expectedCql).matches() && expectedValues.equals(values);
    }

    private Uni<QueryOuterClass.Response> execute(
        QueryOuterClass.Batch.Type actualBatchType, boolean actualEnriched) {
      assertThat(this.batchType)
          .as("Batch type for query %s", cqlPattern)
          .isEqualTo(actualBatchType);
      assertThat(this.enriched).isEqualTo(actualEnriched);

      executed();

      QueryOuterClass.ResultSet.Builder resultSet = QueryOuterClass.ResultSet.newBuilder();
      rows.forEach(row -> resultSet.addRows(QueryOuterClass.Row.newBuilder().addAllValues(row)));
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
