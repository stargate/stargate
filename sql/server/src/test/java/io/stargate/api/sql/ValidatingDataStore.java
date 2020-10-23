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
package io.stargate.api.sql;

import io.stargate.db.BatchType;
import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.PreparedStatement.Bound;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.assertj.core.api.Assertions;

public class ValidatingDataStore implements DataStore {
  private final Schema schema;
  private final List<QueryExpectation> expectedQueries = new ArrayList<>();
  private final List<Prepared> prepared = new ArrayList<>();
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
  public CompletableFuture<ResultSet> query(
      String queryString, UnaryOperator<Parameters> parametersModifier, Object... values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<PreparedStatement> prepare(String queryString) {
    expectedQueries.stream()
        .filter(qe -> qe.matches(queryString))
        .findAny()
        .orElseGet(() -> Assertions.fail("Unexpected query: " + queryString));

    Prepared p = new Prepared(queryString);
    prepared.add(p);
    return CompletableFuture.completedFuture(p);
  }

  @Override
  public CompletableFuture<ResultSet> batch(
      List<Bound> statements, BatchType batchType, UnaryOperator<Parameters> parametersModifier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<ResultSet> batch(List<String> queries) {
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

  private class Prepared implements PreparedStatement {
    private final String actualCql;
    private boolean bound;

    private Prepared(String actualCql) {
      this.actualCql = actualCql;
    }

    @Override
    public String preparedQueryString() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Bound bind(Object... values) {
      QueryExpectation expectation =
          expectedQueries.stream()
              .filter(qe -> qe.matches(actualCql, values))
              .findFirst()
              .orElseGet(
                  () ->
                      Assertions.fail(
                          "Unexpected query: "
                              + actualCql
                              + " with params: "
                              + Arrays.toString(values)));

      ExpectedExecution exec = new ExpectedExecution(this, expectation);
      expectedExecutions.add(exec);
      bound = true;
      return exec;
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

    public void returningNothing() {
      returning(Collections.emptyList());
    }

    public void returning(List<Map<String, Object>> rows) {
      this.rows = rows;
    }

    private boolean matches(String cql) {
      return cqlPattern.matcher(cql).matches();
    }

    private boolean matches(String cql, Object... values) {
      return matches(cql) && (params == null || Arrays.equals(params, values));
    }
  }

  private class ExpectedExecution implements Bound {

    private final Prepared query;
    private final QueryExpectation expectation;
    private boolean executed;

    public ExpectedExecution(Prepared query, QueryExpectation expectation) {
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

    @Override
    public CompletableFuture<ResultSet> execute(UnaryOperator<Parameters> parametersModifier) {
      executed = true;
      return CompletableFuture.completedFuture(
          ListBackedResultSet.of(expectation.table, expectation.rows));
    }

    @Override
    public PreparedStatement preparedStatement() {
      return query;
    }

    @Override
    public List<Object> values() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BoundStatement toPersistenceStatement(ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException();
    }
  }
}
