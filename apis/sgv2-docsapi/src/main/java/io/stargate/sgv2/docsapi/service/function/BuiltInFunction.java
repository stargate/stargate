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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.service.function;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import java.util.List;

/**
 * Basic interface for built-in function implementation. Each function must provide an unique
 * operation identifier using the {@link #getOperation()} method.
 */
public interface BuiltInFunction {

  /** @return Returns the operation, for example <code>$push</code>. */
  String getOperation();

  /**
   * Executes this function, returning the result that should be sent back to the caller. Function
   * should return <code>null</code>, in case the document to perform the function is not found.
   *
   * @param table Table schema
   * @param data Represents data for the function execution.
   * @return
   */
  Uni<JsonNode> execute(Uni<Schema.CqlTable> table, BuiltInFunction.Data data);

  /**
   * Simple record that describes data needed for any function execution.
   *
   * @param namespace Namespace to execute the function for
   * @param collection Collection to execute the function for
   * @param documentId Document id to execute the function for
   * @param documentPath Document sub-path
   * @param executionContext Execution context
   * @param input Function input
   */
  record Data(
      String namespace,
      String collection,
      String documentId,
      List<String> documentPath,
      ExecutionContext executionContext,
      JsonNode input) {}
}
