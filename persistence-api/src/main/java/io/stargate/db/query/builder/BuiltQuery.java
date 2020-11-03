package io.stargate.db.query.builder;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.TypedValue;
import io.stargate.db.schema.Column.ColumnType;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.utils.MD5Digest;

public abstract class BuiltQuery<B extends AbstractBound<?>> implements Query<B> {
  private final QueryType queryType;
  private final TypedValue.Codec codec;
  private final @Nullable MD5Digest preparedId;
  private final List<BindMarker> bindMarkers;
  private final AsyncQueryExecutor executor;

  protected BuiltQuery(
      QueryType queryType,
      TypedValue.Codec codec,
      @Nullable MD5Digest preparedId,
      AsyncQueryExecutor executor,
      List<BindMarker> bindMarkers) {
    this.queryType = queryType;
    this.codec = codec;
    this.preparedId = preparedId;
    this.bindMarkers = bindMarkers;
    this.executor = executor;
  }

  AsyncQueryExecutor executor() {
    return executor;
  }

  /** The type of query this is. */
  public QueryType type() {
    return queryType;
  }

  @Override
  public TypedValue.Codec valueCodec() {
    return codec;
  }

  @Override
  public Optional<MD5Digest> preparedId() {
    return Optional.ofNullable(preparedId);
  }

  @Override
  public List<BindMarker> bindMarkers() {
    return bindMarkers;
  }

  @Override
  public final B bindValues(List<TypedValue> values) {
    Preconditions.checkArgument(
        values.size() == bindMarkers.size(),
        "Unexpected number of arguments. Expected %s but got %s. Statement: %s.",
        bindMarkers.size(),
        values.size(),
        queryStringForPreparation());
    return createBoundQuery(values);
  }

  protected abstract B createBoundQuery(List<TypedValue> values);

  protected TypedValue createTyped(String description, ColumnType type, Object value) {
    return TypedValue.forJavaValue(valueCodec(), description, type, value);
  }

  private int externalIndex(Value<?> marker) {
    return ((Value.Marker<?>) marker).externalIndex();
  }

  protected TypedValue convertValue(
      Value<?> value, String receiverName, ColumnType type, List<TypedValue> externalBoundValues) {
    return value.isMarker()
        ? externalBoundValues.get(externalIndex(value))
        : createTyped(receiverName, type, value.get());
  }

  /**
   * Binds and executes this prepared statement.
   *
   * <p>This is a shortcut for {@link #execute(UnaryOperator, Object...)} but where the execution
   * {@link Parameters} are the default ones of the underlying data store.
   */
  public CompletableFuture<ResultSet> execute(Object... values) {
    return execute(p -> p, values);
  }

  /**
   * Binds and executes this prepared statement.
   *
   * <p>This is a shortcut for {@link #execute(UnaryOperator, Object...)} where the underlying data
   * store default parameters are only modified to use the provided consistency level.
   */
  public CompletableFuture<ResultSet> execute(ConsistencyLevel consistency, Object... values) {
    return execute(p -> p.withConsistencyLevel(consistency), values);
  }

  /**
   * Binds and executes this prepared statement.
   *
   * @param parametersModifier a function called on the default parameters of then underlying data
   *     store (the instance provided when building the data store) and whose result parameters are
   *     used for the statement execution.
   * @param values the (positional) values for the bind variables in this query if any.
   * @return a future with a {@link ResultSet} object to access the result of the query. Please see
   *     {@link AsyncQueryExecutor#execute(BoundQuery, UnaryOperator)} for warnings regarding this
   *     future and its result; the same applies here.
   */
  public CompletableFuture<ResultSet> execute(
      UnaryOperator<Parameters> parametersModifier, Object... values) {
    return bind(values).execute(parametersModifier);
  }
}
