/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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

import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.transport.ProtocolVersion;

/** A statement prepared through the {@link DataStore} API. */
public interface PreparedStatement {

  String preparedQueryString();

  /**
   * Executes this prepared statement.
   *
   * <p>This is a shortcut for {@link #execute(UnaryOperator, Object...)} but where the execution
   * {@link Parameters} are the default ones of the underlying data store.
   */
  default CompletableFuture<ResultSet> execute(Object... values) {
    return execute(p -> p, values);
  }

  /**
   * Executes this prepared statement.
   *
   * <p>This is a shortcut for {@link #execute(UnaryOperator, Object...)} where the underlying data
   * store default parameters are only modified to use the provided consistency level.
   */
  default CompletableFuture<ResultSet> execute(ConsistencyLevel consistency, Object... values) {
    return execute(p -> p.withConsistencyLevel(consistency), values);
  }

  /**
   * Executes this prepared statement.
   *
   * @param parametersModifier a function called on the default parameters of then underlying data
   *     store (the instance provided when building the data store) and whose result parameters are
   *     used for the statement execution.
   * @param values the (positional) values for the bind variables in this prepared statement, if
   *     any.
   * @return a future with a {@link ResultSet} object to access the result of the query. Please see
   *     {@link Bound#execute(UnaryOperator)} for warnings regarding this future and its result; the
   *     same applies here.
   */
  default CompletableFuture<ResultSet> execute(
      UnaryOperator<Parameters> parametersModifier, Object... values) {
    return bind(values).execute(parametersModifier);
  }

  /**
   * Bounds the bind markers of this prepared statement to the provided values.
   *
   * @param values the value to bind (positionally) to this prepared statement bind variables.
   * @return a bound object grouping this prepared statements to its bound {@code values}.
   */
  Bound bind(Object... values);

  /** Groups a prepared statement and its bound values. */
  interface Bound {

    PreparedStatement preparedStatement();

    List<Object> values();

    /**
     * Executes this bound statement.
     *
     * <p>This is a shortcut for {@link #execute(UnaryOperator)} but where the execution {@link
     * Parameters} are the default ones of the underlying data store.
     */
    default CompletableFuture<ResultSet> execute() {
      return execute(p -> p);
    }

    /**
     * Executes this bound statement.
     *
     * <p>This is a shortcut for {@link #execute(UnaryOperator)} where the underlying data store
     * default parameters are only modified to use the provided consistency level.
     */
    default CompletableFuture<ResultSet> execute(ConsistencyLevel consistency) {
      return execute(p -> p.withConsistencyLevel(consistency));
    }

    /**
     * Executes this bound statement.
     *
     * @param parametersModifier a function called on the default parameters of then underlying data
     *     store (the instance provided when building the data store) and whose result parameters
     *     are used for the statement execution.
     * @return a future with a {@link ResultSet} object to access the result of the query. The
     *     future is complete as soon as some initial result for the query is available, which for
     *     paging queries means only the first page of the result set. As for {@link
     *     Persistence.Connection#execute}, this future can be completed on a sensitive thread and
     *     one should not chain blocking operations on this future <b>including</b> iterating over
     *     the whole result set (as this may be block when paging kicks in to query further pages).
     *     In other words, <b>do not</b> do:
     *     <pre>
     *   query(...).thenAccept(rs -> { for (Row r : rs) {...} });
     * </pre>
     *     Use {@link CompletableFuture#thenAcceptAsync} instead in that case.
     */
    CompletableFuture<ResultSet> execute(UnaryOperator<Parameters> parametersModifier);

    /**
     * Creates a low-level {@link BoundStatement} (for execution against a {@link Persistence}
     * instance) equivalent to bound statement.
     *
     * @param protocolVersion the protocol version to use to serialize the values.
     * @return the created statement.
     */
    BoundStatement toPersistenceStatement(ProtocolVersion protocolVersion);
  }
}
