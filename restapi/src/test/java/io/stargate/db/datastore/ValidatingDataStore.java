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

import io.stargate.db.BatchType;
import io.stargate.db.Parameters;
import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.assertj.core.api.Assertions;

public class ValidatingDataStore implements DataStore {
  private final Schema schema;
  private final List<QueryExpectation> expectedQueries = new ArrayList<>();
  private final List<Prepared<?>> prepared = new ArrayList<>();
  private final List<ExpectedExecution> expectedExecutions = new ArrayList<>();

  public ValidatingDataStore(Schema schema) {
    this.schema = schema;
  }

  public void reset() {
    prepared.clear();
    expectedExecutions.clear();
    expectedQueries.clear();
  }

  public void ignorePrepared() {
    prepared.clear();
  }

  public void validate() {
    prepared.forEach(Prepared::validate);
    expectedExecutions.forEach(ExpectedExecution::validate);
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

  @Override
  public <B extends BoundQuery> CompletableFuture<Query<B>> prepare(Query<B> query) {
    String queryString = query.queryStringForPreparation();
    expectedQueries.stream()
        .filter(qe -> qe.matches(queryString))
        .findAny()
        .orElseGet(() -> Assertions.fail("Unexpected query: " + queryString));

    Prepared<B> p = new Prepared<>(query);
    prepared.add(p);
    return CompletableFuture.completedFuture(p);
  }

  @Override
  public CompletableFuture<ResultSet> execute(
      BoundQuery query, UnaryOperator<Parameters> parametersModifier) {
    ExpectedExecution execution;
    if (query instanceof ExpectedExecution) {
      execution = (ExpectedExecution) query;
    } else {
      Prepared<?> prepared = (Prepared<?>) prepare(query.source().query()).join();
      execution = (ExpectedExecution) prepared.bindValues(query.values());
    }

    Parameters parameters = parametersModifier.apply(Parameters.defaults());
    return execution.execute(parameters);
  }

  @Override
  public CompletableFuture<ResultSet> batch(
      Collection<BoundQuery> queries,
      BatchType batchType,
      UnaryOperator<Parameters> parametersModifier) {
    throw new UnsupportedOperationException();
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

  private class Prepared<B extends BoundQuery> implements Query<B> {
    private final String actualCql;
    private final Query<B> query;
    private boolean bound;

    private Prepared(Query<B> query) {
      this.actualCql = query.queryStringForPreparation();
      this.query = query;
    }

    @Override
    public Codec valueCodec() {
      return Codec.testCodec();
    }

    @Override
    public List<BindMarker> bindMarkers() {
      return query.bindMarkers();
    }

    @Override
    public Optional<MD5Digest> preparedId() {
      return Optional.empty();
    }

    @Override
    public Query<B> withPreparedId(MD5Digest preparedId) {
      return this;
    }

    @Override
    public String queryStringForPreparation() {
      return actualCql;
    }

    @Override
    public B bindValues(List<TypedValue> values) {
      QueryExpectation expectation =
          expectedQueries.stream()
              .filter(qe -> qe.matches(actualCql, values))
              .findFirst()
              .orElseGet(
                  () ->
                      Assertions.fail(
                          "Unexpected query: " + actualCql + " with params: " + values));

      ExpectedExecution exec = new ExpectedExecution(this, expectation);
      expectedExecutions.add(exec);
      bound = true;
      //noinspection unchecked
      return (B) exec;
    }

    public void validate() {
      if (!bound) {
        Assertions.fail("The following query was prepared but never bound: " + actualCql);
      }
    }
  }

  public static class QueryExpectation {

    private final Table table;
    private final Pattern cqlPattern;
    private final Object[] params;
    private int pageSize = Integer.MAX_VALUE;
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

    public void returningNothing() {
      returning(Collections.emptyList());
    }

    public void returning(List<Map<String, Object>> rows) {
      this.rows = rows;
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
  }

  private class ExpectedExecution implements BoundQuery {

    private final Prepared<?> query;
    private final QueryExpectation expectation;
    private boolean executed;

    public ExpectedExecution(Prepared<?> query, QueryExpectation expectation) {
      this.query = query;
      this.expectation = expectation;
    }

    private void validate() {
      if (!executed) {
        Assertions.fail(
            "The following query was prepared but not executed: "
                + query.actualCql
                + ", params: "
                + Arrays.toString(expectation.params));
      }
    }

    private CompletableFuture<ResultSet> execute(Parameters parameters) {
      executed = true;

      Optional<ByteBuffer> pagingState = parameters.pagingState();
      ValidatingPaginator paginator = ValidatingPaginator.of(expectation.pageSize, pagingState);

      return CompletableFuture.supplyAsync(
          () -> ListBackedResultSet.of(expectation.table, expectation.rows, paginator));
    }

    @Override
    public QueryType type() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Source<?> source() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<TypedValue> values() {
      throw new UnsupportedOperationException();
    }
  }
}
