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
package io.stargate.grpc.payload;

import com.google.protobuf.Any;
import io.stargate.db.BoundStatement;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.Rows;
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
  BoundStatement bindValues(Prepared prepared, Any payload, ByteBuffer unsetValue) throws Exception;

  /**
   * Convert a {@link Rows} result type into a result payload.
   *
   * @param rows The raw CQL native protocol values and column metadata needed to convert to the
   *     resulting payload result.
   * @param parameters Mostly used for {@link QueryParameters#getPopulateMetadata()}, but could be
   *     used to control other payload-specific features in the future.
   * @return A payload built from the raw CQL native protocol values.
   * @throws Exception
   */
  Any processResult(Rows rows, QueryParameters parameters) throws Exception;
}
