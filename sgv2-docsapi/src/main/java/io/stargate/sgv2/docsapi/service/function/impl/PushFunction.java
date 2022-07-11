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

/**
 * Push function implementation. Pushes a value to the existing array. Returns the updated array.
 */
@ApplicationScoped
public class PushFunction implements BuiltInFunction {

  @Inject ReadDocumentsService readBridgeService;

  @Inject WriteDocumentsService writeDocumentsService;

  /** {@inheritDoc} */
  @Override
  public String getOperation() {
    return "$push";
  }

  /** {@inheritDoc} */
  @Override
  public Uni<JsonNode> execute(Uni<Schema.CqlTable> table, Data data) {
    // get the doc at the path
    return readBridgeService
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
                        "The path provided to push to has no array, found %s.".formatted(jsonNode));
                return Uni.createFrom().failure(invalid);
              }

              // push value
              ArrayNode arrayNode = (ArrayNode) jsonNode;
              arrayNode.insert(arrayNode.size(), data.input());

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

                  // return updated array back
                  .map(any -> arrayNode);
            });
  }
}
