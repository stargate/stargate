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

package io.stargate.sgv2.docsapi.service.query.condition;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.query.condition.provider.ConditionProvider;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterHintCode;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterOperationCode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Simple service that wraps all available raw filter values and connects them to a {@link
 * ConditionProvider}, enabling parsing of the filter nodes.
 */
@ApplicationScoped
public class ConditionParser {

  private final DocumentProperties documentProperties;

  @Inject
  public ConditionParser(DocumentProperties documentProperties) {
    this.documentProperties = documentProperties;
  }

  /**
   * Creates the conditions for the node containing the raw filter ops as the keys. For example:
   * <code>{ "$gt: { 5 }, "$lt": { 10 }}</code>.
   *
   * @param conditionsNode Node containing the filter ops as keys
   * @param numericBooleans If number booleans should be used when creating conditions.
   * @return Collection of created conditions.
   * @throws ErrorCodeRuntimeException If filter op is not found, condition constructions fails or
   *     filter value is not supported by the filter op.
   */
  public Collection<BaseCondition> getConditions(JsonNode conditionsNode, boolean numericBooleans) {
    if (!conditionsNode.isObject()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID);
    }

    List<BaseCondition> results = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> fields = conditionsNode.fields();
    fields.forEachRemaining(
        field -> {
          String filterOp = field.getKey();

          // skip hints (processed elsewhere)
          if (FilterHintCode.getByRawValue(filterOp).isPresent()) {
            return;
          }

          Optional<FilterOperationCode> operationCode = FilterOperationCode.getByRawValue(filterOp);
          if (operationCode.isPresent()) {
            JsonNode valueNode = field.getValue();
            FilterOperationCode code = operationCode.get();
            Optional<? extends BaseCondition> condition =
                code.getConditionProvider()
                    .createCondition(valueNode, documentProperties, numericBooleans);
            if (condition.isPresent()) {
              results.add(condition.get());
            } else {
              // condition empty
              String msg =
                  String.format(
                      "Operation '%s' does not support the provided value %s.",
                      filterOp, valueNode.toPrettyString());
              throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
            }
          } else {
            // provider can not be found
            String msg = String.format("Operation '%s' is not supported.", filterOp);
            throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
          }
        });
    return results;
  }
}
