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

package io.stargate.sgv2.docsapi.service.function.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.function.BuiltInFunction;
import io.stargate.sgv2.docsapi.service.query.ReadDocumentsService;
import io.stargate.sgv2.docsapi.service.write.WriteDocumentsService;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Pop function implementation. Pops a value to the existing array. Returns the popped value. */
@ApplicationScoped
public class PopFunction implements BuiltInFunction {

  @Inject ReadDocumentsService readDocumentsService;

  @Inject WriteDocumentsService writeDocumentsService;

  /** {@inheritDoc} */
  @Override
  public String getOperation() {
    return "$pop";
  }

  /** {@inheritDoc} */
  @Override
  public Uni<JsonNode> execute(Uni<Schema.CqlTable> table, BuiltInFunction.Data data) {
    // get the doc at the path
    return readDocumentsService
        .getDocument(
            data.namespace(),
            data.collection(),
            data.documentId(),
            data.documentPath(),
            null,
            data.executionContext())

        // when we get it back
        .onItem()
        .ifNotNull()
        .transformToUni(
            document -> {

              // first ensure array exists
              JsonNode jsonNode = document.data();
              if (jsonNode == null || !jsonNode.isArray()) {
                Exception invalid =
                    new ErrorCodeRuntimeException(
                        ErrorCode.DOCS_API_SEARCH_ARRAY_PATH_INVALID,
                        "The path provided to pop from has no array, found %s."
                            .formatted(jsonNode));
                return Uni.createFrom().failure(invalid);
              }

              // then ensure not empty
              if (jsonNode.isEmpty()) {
                Exception outOfBounds =
                    new ErrorCodeRuntimeException(ErrorCode.DOCS_API_ARRAY_POP_OUT_OF_BOUNDS);
                return Uni.createFrom().failure(outOfBounds);
              }

              // pop one
              ArrayNode arrayNode = (ArrayNode) jsonNode;
              JsonNode value = arrayNode.remove(arrayNode.size() - 1);

              // write sub document with new array state
              // ttl auto is one here as in V1
              return writeDocumentsService
                  .updateSubDocument(
                      table,
                      data.namespace(),
                      data.collection(),
                      data.documentId(),
                      data.documentPath(),
                      arrayNode,
                      true,
                      data.executionContext())

                  // return popped value back
                  .map(any -> value);
            });
  }
}
