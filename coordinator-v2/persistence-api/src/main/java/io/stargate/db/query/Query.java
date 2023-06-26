package io.stargate.db.query;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.cassandra.stargate.exceptions.InvalidRequestException;
import org.apache.cassandra.stargate.utils.MD5Digest;

/**
 * A CQL query.
 *
 * <p>Implementations of this class make no strong guarantees on the validity of the query they
 * represent; executing them may raise an {@link InvalidRequestException}.
 */
public interface Query<B extends BoundQuery> {

  TypedValue.Codec valueCodec();

  /** The (potentially empty) list of bind markers that remains to be bound in this query. */
  List<BindMarker> bindMarkers();

  /** If this is a prepared query, it's prepared ID. */
  Optional<MD5Digest> preparedId();

  /** Creates a prepared copy of this query, using the provided prepared ID. */
  Query<B> withPreparedId(MD5Digest preparedId);

  /**
   * The query string that must be used to prepare this query.
   *
   * <p>Please note that the query string returned by this method may or may not be equal to the one
   * of {@link #toString()} (in particular, the query is allowed to prepare some of values that are
   * not part of {@link #bindMarkers()}, to optimize prepared statement caching for instance).
   */
  String queryStringForPreparation();

  B bindValues(List<TypedValue> values);

  default B bind(Object... values) {
    return bind(Arrays.asList(values));
  }

  default B bind(List<Object> values) {
    return bindValues(TypedValue.forJavaValues(valueCodec(), bindMarkers(), values));
  }

  default B bindBuffers(ByteBuffer... values) {
    return bindBuffers(Arrays.asList(values));
  }

  default B bindBuffers(List<ByteBuffer> values) {
    return bindValues(TypedValue.forBytesValues(valueCodec(), bindMarkers(), values));
  }

  /**
   * A valid CQL query string representation of this query (with bind markers for the values of
   * {@link #bindMarkers()}).
   */
  @Override
  String toString();
}
