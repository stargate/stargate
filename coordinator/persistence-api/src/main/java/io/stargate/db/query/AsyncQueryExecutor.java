package io.stargate.db.query;

import io.stargate.db.BatchType;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.ResultSet;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public interface AsyncQueryExecutor {

  /**
   * Executes the provided query against this executor.
   *
   * <p>This is a shortcut for {@link #execute(BoundQuery, UnaryOperator)} but where the execution
   * {@link Parameters} are the default ones of the executor.
   */
  default CompletableFuture<ResultSet> execute(BoundQuery query) {
    return execute(query, p -> p);
  }

  /**
   * Executes the provided query against this executor.
   *
   * <p>This is a shortcut for {@link #execute(BoundQuery, UnaryOperator)} where the executor
   * default parameters are only modified to use the provided consistency level.
   */
  default CompletableFuture<ResultSet> execute(
      BoundQuery query, ConsistencyLevel consistencyLevel) {
    return execute(query, p -> p.withConsistencyLevel(consistencyLevel));
  }

  /**
   * Executes the provided query against this executor.
   *
   * @param query the query to execute.
   * @param parametersModifier a function called on the default parameters of this executor and
   *     whose result parameters are used for the query execution.
   * @return a future with a {@link ResultSet} object to access the result of the query. The future
   *     is complete as soon as some initial result for the query is available, which for paging
   *     queries means only the first page of the result set. As for {@link
   *     Persistence.Connection#execute}, this future can be completed on a sensitive thread and one
   *     should not chain blocking operations on this future <b>including</b> iterating over the
   *     whole result set (as this may be block when paging kicks in to query further pages). In
   *     other words, <b>do not</b> do:
   *     <pre>
   *   query(...).thenAccept(rs -> { for (Row r : rs) {...} });
   * </pre>
   *     Use {@link CompletableFuture#thenAcceptAsync} instead in that case.
   */
  CompletableFuture<ResultSet> execute(
      BoundQuery query, UnaryOperator<Parameters> parametersModifier);

  /**
   * Executes the provided queries as a batch against this executor.
   *
   * <p>This is a shortcut for {@link #batch(Collection, UnaryOperator)} but where the execution
   * {@link Parameters} are the default ones of the executor.
   */
  default CompletableFuture<ResultSet> batch(Collection<BoundQuery> queries) {
    return batch(queries, p -> p);
  }

  /**
   * Executes the provided queries as an unlogged batch against this executor with default
   * parameters.
   */
  default CompletableFuture<ResultSet> unloggedBatch(Collection<BoundQuery> queries) {
    return batch(queries, BatchType.UNLOGGED, p -> p);
  }

  /**
   * Executes the provided queries as a batch against this executor.
   *
   * <p>This is a shortcut for {@link #batch(Collection, UnaryOperator)} where executor default
   * parameters are only modified to use the provided consistency level.
   */
  default CompletableFuture<ResultSet> batch(
      Collection<BoundQuery> queries, ConsistencyLevel consistencyLevel) {
    return batch(queries, p -> p.withConsistencyLevel(consistencyLevel));
  }

  /**
   * Executes the provided queries as an unlogged batch against this executor.
   *
   * <p>This is a shortcut for {@link #batch(Collection, UnaryOperator)} where executor default
   * parameters are only modified to use the provided consistency level.
   */
  default CompletableFuture<ResultSet> unloggedBatch(
      Collection<BoundQuery> queries, ConsistencyLevel consistencyLevel) {
    return batch(queries, BatchType.UNLOGGED, p -> p.withConsistencyLevel(consistencyLevel));
  }

  /**
   * Executes the provided queries as a batch against this executor.
   *
   * <p>This is a shortcut for {@link #batch(Collection, BatchType, UnaryOperator)} where batch type
   * defaults to "logged".
   */
  default CompletableFuture<ResultSet> batch(
      Collection<BoundQuery> queries, UnaryOperator<Parameters> parametersModifier) {
    return batch(queries, BatchType.LOGGED, parametersModifier);
  }

  /**
   * Executes the provided queries as a batch against this executor.
   *
   * @param queries the queries to execute as a batch.
   * @param batchType the type of the batch.
   * @param parametersModifier a function called on the default parameters of this data store (the
   *     instance provided when building the data store) and whose result parameters are used for
   *     the query execution.
   * @return a future with a {@link ResultSet} object to access the result of the query. See {@link
   *     #execute(BoundQuery, UnaryOperator)} javadoc for details on that result set; the same
   *     description applies here.
   */
  CompletableFuture<ResultSet> batch(
      Collection<BoundQuery> queries,
      BatchType batchType,
      UnaryOperator<Parameters> parametersModifier);
}
