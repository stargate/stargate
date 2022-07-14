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
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.v2.model.dto.DocumentResponseWrapper;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import java.util.List;
import java.util.Objects;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

/**
 * Service that executes document build-in functions. Currently supported:
 *
 * <ol>
 *   <li><code>$pop</code>, implemented in {@link
 *       io.stargate.sgv2.docsapi.service.function.impl.PopFunction}
 *   <li><code>$push</code>, implemented in {@link
 *       io.stargate.sgv2.docsapi.service.function.impl.PushFunction}
 * </ol>
 *
 * @see BuiltInFunction
 */
@ApplicationScoped
public class FunctionService {

  // NOTE: Fully tested via the BuiltInFunctionResourceIntegrationTest

  @Inject Instance<BuiltInFunction> functions;

  /**
   * Executes a built-in function.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Function operation does not exists, with {@link
   *       ErrorCode#DOCS_API_INVALID_BUILTIN_FUNCTION}
   *   <li>Function execution failed, with any code
   * </ol>
   *
   * @param namespace Namespace to execute the function for
   * @param collection Collection to execute the function for
   * @param documentId Document id to execute the function for
   * @param subPath Document sub-path
   * @param operation The function operation name.
   * @param input Function input (optional, depends on the function).
   * @param context Execution content
   * @return Result of function invocation (depends on the function).
   */
  public Uni<DocumentResponseWrapper<JsonNode>> executeBuiltInFunction(
      Uni<Schema.CqlTable> table,
      String namespace,
      String collection,
      String documentId,
      List<String> subPath,
      String operation,
      JsonNode input,
      ExecutionContext context) {

    // get the function
    return findFunction(operation)

        // if there, construct the data and call
        .flatMap(
            f -> {
              BuiltInFunction.Data data =
                  new BuiltInFunction.Data(
                      namespace, collection, documentId, subPath, context, input);
              return f.execute(table, data);
            })

        // if we get the result back, transform to the wrapper
        .onItem()
        .ifNotNull()
        .transform(
            result -> new DocumentResponseWrapper<>(documentId, null, result, context.toProfile()));
  }

  // finds a function, fails if not available
  private Uni<BuiltInFunction> findFunction(String function) {
    return Multi.createFrom()
        .iterable(functions)
        .filter(f -> Objects.equals(f.getOperation(), function))
        .toUni()

        // if not there error our
        .onItem()
        .ifNull()
        .failWith(
            new ErrorCodeRuntimeException(
                ErrorCode.DOCS_API_INVALID_BUILTIN_FUNCTION,
                "No BuiltInApiFunction found for name: " + function));
  }
}
