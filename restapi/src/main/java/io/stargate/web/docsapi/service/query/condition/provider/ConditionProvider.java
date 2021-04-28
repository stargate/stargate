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

package io.stargate.web.docsapi.service.query.condition.provider;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import java.util.Optional;

/** Knows how to provide {@link BaseCondition} from the Json node containing the filter value. */
public interface ConditionProvider {

  /**
   * Optionally returns the {@link BaseCondition} for the given value node. Note that this should be
   * a value from the filter object, f.e. given <code>{ "eq": "Honda" }</code> this should refer to
   * <code>"Honda"</code>
   *
   * @param node Value node from the filter op.
   * @param numericBooleans If booleans should be considered as numberic values.
   * @return Optionally {@link BaseCondition}
   */
  Optional<? extends BaseCondition> createCondition(JsonNode node, boolean numericBooleans);
}
