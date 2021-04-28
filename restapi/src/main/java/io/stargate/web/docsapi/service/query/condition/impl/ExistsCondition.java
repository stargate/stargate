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

package io.stargate.web.docsapi.service.query.condition.impl;

import io.stargate.db.datastore.Row;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.filter.operation.FilterOperationCode;
import org.immutables.value.Value;

import java.util.Optional;

/**
 * Exists condition is special type of the condition, as it does not provide {@link io.stargate.db.query.builder.BuiltCondition},
 * but is considered as the persistence condition. Also it has no filter operation and resolves each row test to true.
 */
@Value.Immutable
public abstract class ExistsCondition implements BaseCondition {

  /** @return Filter query value. */
  @Value.Parameter
  public abstract boolean getQueryValue();

  /** Validates the filter value as we only accept true */
  @Value.Check
  protected void validate() {
    boolean queryValue = getQueryValue();
    if (!Boolean.TRUE.equals(queryValue)) {
      String msg = String.format("%s only supports the value `true`", FilterOperationCode.EXISTS.getRawValue());
      throw new DocumentAPIRequestException(msg);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isPersistenceCondition() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Optional<BuiltCondition> getBuiltCondition() {
    return Optional.empty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean test(Row row) {
    // always test true when row is there :)
    return true;
  }

}
