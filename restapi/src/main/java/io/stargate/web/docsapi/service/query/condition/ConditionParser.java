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

package io.stargate.web.docsapi.service.query.condition;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import io.stargate.web.docsapi.service.query.condition.provider.ConditionProvider;
import io.stargate.web.docsapi.service.query.filter.operation.FilterOperationCode;
import java.util.*;

/**
 * Simple service that wraps all available raw filter values and connects them to a {@link
 * ConditionProvider}, enabling parsing of the filter nodes.
 */
public class ConditionParser {

  /** If number booleans should be used when creating conditions. */
  private final boolean numericBooleans;

  public ConditionParser(boolean numericBooleans) {
    this.numericBooleans = numericBooleans;
  }

  /**
   * Creates the conditions for the node containing the raw filter ops as the keys. For example:
   * <code>{ "$gt: { 5 }, "$lt": { 10 }}</code>.
   *
   * @param conditionsNode Node containing the filter ops as keys
   * @return Collection of created conditions.
   * @throws io.stargate.web.docsapi.exception.DocumentAPIRequestException If filter op is not
   *     found, condition constructions fails or filter value is not supported by the filter op.
   */
  public Collection<BaseCondition> getConditions(JsonNode conditionsNode) {
    List<BaseCondition> results = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> fields = conditionsNode.fields();
    fields.forEachRemaining(
        field -> {
          String filterOp = field.getKey();
          Optional<FilterOperationCode> operationCode = FilterOperationCode.getByRawValue(filterOp);
          if (operationCode.isPresent()) {
            JsonNode valueNode = field.getValue();
            FilterOperationCode code = operationCode.get();
            Optional<? extends BaseCondition> condition =
                code.getConditionProvider().createCondition(valueNode, numericBooleans);
            if (condition.isPresent()) {
              results.add(condition.get());
            } else {
              // condition empty
              String msg =
                  String.format(
                      "Operation %s is not supporting the provided value %s.",
                      filterOp, valueNode.toPrettyString());
              throw new DocumentAPIRequestException(msg);
            }
          } else {
            // provider can not be found
            String msg = String.format("Operation %s is not supported.", filterOp);
            throw new DocumentAPIRequestException(msg);
          }
        });
    return results;
  }
}
