package io.stargate.db.datastore;

import io.stargate.db.Parameters;
import io.stargate.db.query.BoundQuery;
import java.util.Collections;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.immutables.value.Value;

/**
 * Options for a {@link DataStore} instance.
 *
 * <p>Note that options are passed at the time of the {@link DataStore} construction and cannot be
 * changed afterwards (you need to create a new instance to get different options).
 */
@Value.Immutable(singleton = true)
public abstract class DataStoreOptions {

  public static DataStoreOptions defaults() {
    return ImmutableDataStoreOptions.of();
  }

  public static DataStoreOptions defaultsWithAutoPreparedQueries() {
    return builder().alwaysPrepareQueries(true).build();
  }

  public static ImmutableDataStoreOptions.Builder builder() {
    return ImmutableDataStoreOptions.builder();
  }

  /**
   * The default parameters use by the datastore for execution (those can be overridden
   * per-execution, see {@link DataStore#execute(BoundQuery, UnaryOperator)}).
   */
  @Value.Default
  public Parameters defaultParameters() {
    return Parameters.defaults();
  }

  /**
   * If {@code true}, the datastore will internally prepare non-prepared query it is passed before
   * execution.
   *
   * <p>This can save time query parsing when queries are built at execution time (not manually
   * prepared) as query preparation first check if the query isn't already prepared and return
   * immediately if they are (and that check is faster than parsing). Of course, if the queries
   * executed are one-offs, this option is undesirable (adds a bit of latency for the preparation
   * without saving parsing, and can unnecessary pollute the prepared statement cache). But do note
   * that queries built through {@link DataStore#queryBuilder()} always prepare queries with no
   * values at all (even if values are passed at construction time), so this option is unlikely to
   * be very problematic with those queries (at worst, it's a minor inefficiency).
   */
  @Value.Default
  public boolean alwaysPrepareQueries() {
    return false;
  }

  @Value.Default
  public Map<String, String> customProperties() {
    return Collections.emptyMap();
  }

  @Override
  public String toString() {
    return String.format(
        "{defaultParameters: %s, alwaysPrepareQueries: %b, customProperties: %s}",
        defaultParameters(), alwaysPrepareQueries(), customProperties());
  }
}
