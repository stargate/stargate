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

package io.stargate.web.docsapi.service.query.filter.operation;

import io.stargate.web.docsapi.service.query.condition.provider.ConditionProvider;
import io.stargate.web.docsapi.service.query.condition.provider.impl.BasicConditionProvider;
import io.stargate.web.docsapi.service.query.condition.provider.impl.ExistsConditionProvider;
import io.stargate.web.docsapi.service.query.condition.provider.impl.ListConditionProvider;
import io.stargate.web.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.GtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.GteFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.InFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.LteFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.NeFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.NotInFilterOperation;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Maps raw filter values to the condition providers with specific filer operation. */
public enum FilterOperationCode {

  // all existing operations
  EQ("$eq", BasicConditionProvider.of(EqFilterOperation.of())),
  NE("$ne", BasicConditionProvider.of(NeFilterOperation.of())),
  LT("$lt", BasicConditionProvider.of(LtFilterOperation.of())),
  LTE("$lte", BasicConditionProvider.of(LteFilterOperation.of())),
  GT("$gt", BasicConditionProvider.of(GtFilterOperation.of())),
  GTE("$gte", BasicConditionProvider.of(GteFilterOperation.of())),
  EXISTS("$exists", new ExistsConditionProvider()),
  IN("$in", ListConditionProvider.of(InFilterOperation.of())),
  NIN("$nin", ListConditionProvider.of(NotInFilterOperation.of()));

  /** Raw value. */
  private final String rawValue;

  /** Condition provider. */
  private final ConditionProvider conditionProvider;

  FilterOperationCode(String rawValue, ConditionProvider conditionProvider) {
    this.rawValue = rawValue;
    this.conditionProvider = conditionProvider;
  }

  public String getRawValue() {
    return rawValue;
  }

  public ConditionProvider getConditionProvider() {
    return conditionProvider;
  }

  /**
   * Finds a {@link FilterOperationCode} by raw value.
   *
   * @param rawValue
   * @return
   */
  public static Optional<FilterOperationCode> getByRawValue(String rawValue) {
    return Arrays.stream(FilterOperationCode.values())
        .filter(op -> Objects.equals(op.rawValue, rawValue))
        .findFirst();
  }
}
