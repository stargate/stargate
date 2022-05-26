/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.sgv2.docsapi.service.query.condition.provider.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.GenericCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableGenericCondition;
import io.stargate.sgv2.docsapi.service.query.condition.provider.ConditionProvider;
import io.stargate.sgv2.docsapi.service.query.filter.operation.GenericFilterOperation;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Condition provider for the {@link GenericCondition} for lists. Extract objects from the array
 * JSON node.
 */
public class ListConditionProvider implements ConditionProvider {

  /** Filter operation to use in the condition. */
  private final GenericFilterOperation<List<?>> filterOperation;

  public static ListConditionProvider of(GenericFilterOperation<List<?>> predicate) {
    return new ListConditionProvider(predicate);
  }

  public ListConditionProvider(GenericFilterOperation<List<?>> filterOperation) {
    this.filterOperation = filterOperation;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<? extends BaseCondition> createCondition(
      JsonNode node, DocumentProperties documentProperties, boolean numericBooleans) {
    if (node.isArray()) {
      Iterator<JsonNode> iterator = node.iterator();
      List<?> input = getListConditionValues(iterator);
      GenericCondition<List<?>> condition =
          ImmutableGenericCondition.of(filterOperation, input, documentProperties, numericBooleans);
      return Optional.of(condition);
    }
    return Optional.empty();
  }

  private List<?> getListConditionValues(Iterator<JsonNode> iterator) {
    List<Object> result = new ArrayList<>();
    iterator.forEachRemaining(
        node -> {
          // collect supported values
          if (node.isNumber()) {
            result.add(node.numberValue());
          } else if (node.isBoolean()) {
            result.add(node.asBoolean());
          } else if (node.isTextual()) {
            result.add(node.asText());
          } else if (node.isNull()) {
            result.add(null);
          } else {
            // if we hit anything else throw an exception
            String msg =
                String.format(
                    "Operation %s was not expecting a list containing a %s node type.",
                    filterOperation.getOpCode(), node.getNodeType());
            throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
          }
        });
    return result;
  }
}
