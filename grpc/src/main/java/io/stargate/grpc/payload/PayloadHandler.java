package io.stargate.grpc.payload;

import io.stargate.db.BoundStatement;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.Rows;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import java.nio.ByteBuffer;

/** Converts payloads to/from internal representations. */
public interface PayloadHandler {
  /**
   * Convert a prepared query and payload values into a bound statement.
   *
   * @param prepared The prepared statement that contains column metadata used to convert payload
   *     values into CQL native protocol values.
   * @param payload The payload that contains the values to be bound to the prepared statement.
   * @param unsetValue Persistence backend specific value that represents an unset value.
   * @return A statement that has been bound with payload values.
   * @throws Exception
   */
  BoundStatement bindValues(Prepared prepared, Payload payload, ByteBuffer unsetValue)
      throws Exception;

  /**
   * Convert a {@link Rows} result type into a result payload.
   *
   * @param rows The raw CQL native protocol values and column metadata needed to convert to the
   *     resulting payload result.
   * @param parameters Mostly used for {@link QueryParameters#getSkipMetadata()}, but could be used
   *     to control other payload-specific features in the future.
   * @return A payload built from the raw CQL native protocol values.
   * @throws Exception
   */
  Payload processResult(Rows rows, QueryParameters parameters) throws Exception;
}
