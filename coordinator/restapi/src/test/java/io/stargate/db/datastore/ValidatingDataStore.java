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
package io.stargate.db.datastore;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.db.BatchType;
import io.stargate.db.Parameters;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.assertj.core.api.AbstractIntegerAssert;
import org.assertj.core.api.Assertions;

public class ValidatingDataStore implements DataStore {

  private final Schema schema;
  private final AtomicInteger preparedIdSeq = new AtomicInteger();
  private final List<QueryExpectation> expectedQueries = new ArrayList<>();
  private final Map<MD5Digest, Prepared> prepared = new HashMap<>();

  public ValidatingDataStore(Schema schema) {
    this.schema = schema;
  }

  public void reset() {
    prepared.clear();
    expectedQueries.clear();
  }

  public void ignorePrepared() {
    prepared.clear();
  }

  public void validate() {
    prepared.values().forEach(Prepared::validate);
    expectedQueries.forEach(QueryExpectation::validate);
  }

  @Override
  public Codec valueCodec() {
    return Codec.testCodec();
  }

  @Override
  public boolean supportsSecondaryIndex() {
    return true;
  }

  @Override
  public boolean supportsSAI() {
    return false;
  }

  @Override
  public boolean supportsLoggedBatches() {
    return false;
  }

  private MD5Digest nextQueryId() {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.putInt(preparedIdSeq.incrementAndGet());
    return MD5Digest.wrap(buffer.array());
  }

  private <B extends BoundQuery> MD5Digest prepareInternal(Query<B> query) {
    String queryString = query.queryStringForPreparation();
    expectedQueries.stream()
        .filter(qe -> qe.matches(queryString))
        .findAny()
        .orElseGet(
            () -> {
              String expectedQueries =
                  this.expectedQueries.stream()
                      .map(e -> e.cqlPattern.toString())
                      .collect(Collectors.joining("\n"));
              return Assertions.fail(
                  "Unexpected query: "
                      + queryString
                      + "\n Expecting one of the following queries:\n"
                      + expectedQueries);
            });

    // Generate unique IDs for each `prepare` call to validate that all prepared statements get
    // executed.
    MD5Digest preparedId = nextQueryId();

    prepared.put(preparedId, new Prepared(queryString));
    return preparedId;
  }

  @Override
  public <B extends BoundQuery> CompletableFuture<Query<B>> prepare(Query<B> query) {
    MD5Digest preparedId = prepareInternal(query);

    return CompletableFuture.completedFuture(query.withPreparedId(preparedId));
  }

  @Override
  public CompletableFuture<ResultSet> execute(
      BoundQuery query, UnaryOperator<Parameters> parametersModifier) {
    return executeInternal(query, parametersModifier, null);
  }

  private CompletableFuture<ResultSet> executeInternal(
      BoundQuery query, UnaryOperator<Parameters> parametersModifier, BatchType batchType) {
    MD5Digest preparedId =
        query
            .source()
            .query()
            .preparedId()
            .orElseGet(() -> prepareInternal(query.source().query()));

    Prepared prepared =
        this.prepared.computeIfAbsent(
            preparedId, d -> Assertions.fail("Unknown prepared ID: " + d));

    QueryExpectation expectation = prepared.bindValues(query.values());

    Parameters parameters = parametersModifier.apply(Parameters.defaults());
    return expectation.execute(parameters, batchType);
  }

  @Override
  public CompletableFuture<ResultSet> batch(
      Collection<BoundQuery> queries,
      BatchType batchType,
      UnaryOperator<Parameters> parametersModifier) {
    CompletableFuture<ResultSet> last = null;
    for (BoundQuery query : queries) {
      last = executeInternal(query, p -> p, batchType);
    }

    assertThat(last).isNotNull();
    return last;
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public boolean isInSchemaAgreement() {
    return true;
  }

  @Override
  public void waitForSchemaAgreement() {}

  private QueryExpectation add(QueryExpectation expectation) {
    expectedQueries.add(expectation);
    return expectation;
  }

  public QueryExpectation withQuery(Table table, String cql, Object... params) {
    cql = String.format(cql, table.keyspace() + "." + table.name());
    return add(new QueryExpectation(table, Pattern.quote(cql), params));
  }

  protected QueryExpectation withAnySelectFrom(Table table) {
    String regex = "SELECT.*FROM.*" + table.keyspace() + "\\." + table.name() + ".*";
    return add(new QueryExpectation(table, regex, new Object[0]));
  }

  protected QueryExpectation withAnyUpdateOf(Table table) {
    String regex = "UPDATE.*" + table.keyspace() + "\\." + table.name() + ".*";
    return add(new QueryExpectation(table, regex));
  }

  protected QueryExpectation withAnyInsertInfo(Table table) {
    String regex = "INSERT INTO.*" + table.keyspace() + "\\." + table.name() + ".*";
    return add(new QueryExpectation(table, regex));
  }

  protected QueryExpectation withAnyDeleteFrom(Table table) {
    String regex = "DELETE.*FROM.*" + table.keyspace() + "\\." + table.name() + ".*";
    return add(new QueryExpectation(table, regex));
  }

  private class Prepared {
    private final String actualCql;
    private boolean bound;

    private Prepared(String actualCql) {
      this.actualCql = actualCql;
    }

    public QueryExpectation bindValues(List<TypedValue> values) {
      QueryExpectation expectation =
          expectedQueries.stream()
              .filter(qe -> qe.matches(actualCql, values))
              .findFirst()
              .orElseGet(
                  () -> {
                    String expectedQueries =
                        ValidatingDataStore.this.expectedQueries.stream()
                            .map(e -> e.cqlPattern + " with params " + Arrays.toString(e.params))
                            .collect(Collectors.joining(",\n"));
                    return Assertions.fail(
                        "Unexpected query: "
                            + actualCql
                            + " with params: "
                            + values
                            + "\nExpecting one of:\n"
                            + expectedQueries);
                  });

      bound = true;
      return expectation;
    }

    public void validate() {
      if (!bound) {
        Assertions.fail("The following query was prepared but never executed: " + actualCql);
      }
    }
  }

  public abstract static class QueryAssert {

    private final AtomicInteger executeCount = new AtomicInteger();

    public AbstractIntegerAssert<?> assertExecuteCount() {
      return Assertions.assertThat(executeCount.get());
    }

    void executed() {
      executeCount.incrementAndGet();
    }
  }

  public static class QueryExpectation extends QueryAssert {

    private final Table table;
    private final Pattern cqlPattern;
    private final Object[] params;
    private int pageSize = Integer.MAX_VALUE;
    private BatchType batchType;
    private List<Map<String, Object>> rows;

    private QueryExpectation(Table table, String cqlRegEx, Object[] params) {
      this.cqlPattern = Pattern.compile(cqlRegEx);
      this.table = table;
      this.params = params;
    }

    private QueryExpectation(Table table, String cqlRegEx) {
      this.cqlPattern = Pattern.compile(cqlRegEx);
      this.table = table;
      this.params = null;
    }

    public QueryExpectation withPageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public QueryExpectation inBatch(BatchType batchType) {
      this.batchType = batchType;
      return this;
    }

    public QueryAssert returningNothing() {
      return returning(Collections.emptyList());
    }

    public QueryAssert returning(List<Map<String, Object>> rows) {
      this.rows = rows;
      return this;
    }

    private boolean matches(String cql) {
      return cqlPattern.matcher(cql).matches();
    }

    private static Object[] values(List<TypedValue> values) {
      return values.stream().map(TypedValue::javaValue).toArray();
    }

    private boolean matches(String cql, List<TypedValue> values) {
      return matches(cql) && (params == null || Arrays.equals(params, values(values)));
    }

    private CompletableFuture<ResultSet> execute(Parameters parameters, BatchType batchType) {
      Optional<ByteBuffer> pagingState = parameters.pagingState();
      int pageSize;
      if (this.pageSize < Integer.MAX_VALUE) {
        pageSize = this.pageSize;
        assertThat(parameters.pageSize()).hasValue(pageSize);
      } else {
        pageSize = parameters.pageSize().orElse(this.pageSize);
      }

      assertThat(this.batchType)
          .withFailMessage(
              String.format(
                  "Query with pattern %s does not match the expected batch type %s.",
                  cqlPattern, batchType))
          .isEqualTo(batchType);

      ValidatingPaginator paginator = ValidatingPaginator.of(pageSize, pagingState);

      executed();
      return CompletableFuture.completedFuture(ListBackedResultSet.of(table, rows, paginator));
    }

    private void validate() {
      assertExecuteCount()
          .withFailMessage(
              "No queries were executed for this expected pattern: "
                  + cqlPattern
                  + ", params: "
                  + Arrays.toString(params))
          .isGreaterThanOrEqualTo(1);
    }
  }
}
