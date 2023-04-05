package io.stargate.db.query;

import java.util.List;
import java.util.Objects;

public interface BoundQuery {
  /** The type of query this is. */
  QueryType type();

  /** The query and values used to obtain this bound query. */
  BoundQuery.Source<?> source();

  /**
   * A CQL query string representation of this query, with bind markers for the values of {@link
   * #values()}.
   */
  default String queryString() {
    return source().query().queryStringForPreparation();
  }

  /**
   * The values of this bound query, corresponding to the bind markers for {@link #queryString()}.
   *
   * <p>Please note that those values may or may not be equals to {@code bounded().query().values()}
   * because, as specified in {@link Query#queryStringForPreparation()}, a {@link Query} is allowed
   * to include some additional values (that the ones in {@link Query#bindMarkers()}) in the bound
   * queries it produces.
   */
  List<TypedValue> values();

  final class Source<Q extends Query<?>> {
    private final Q sourceQuery;
    private final List<TypedValue> sourceValues;

    public Source(Q boundedQuery, List<TypedValue> sourceValues) {
      this.sourceQuery = boundedQuery;
      this.sourceValues = sourceValues;
    }

    public Q query() {
      return sourceQuery;
    }

    public List<TypedValue> values() {
      return sourceValues;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Source)) {
        return false;
      }
      Source<?> source = (Source<?>) o;
      return sourceQuery.equals(source.sourceQuery) && sourceValues.equals(source.sourceValues);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceQuery, sourceValues);
    }

    @Override
    public String toString() {
      return String.format("%s with values=%s", sourceQuery, sourceValues);
    }
  }
}
