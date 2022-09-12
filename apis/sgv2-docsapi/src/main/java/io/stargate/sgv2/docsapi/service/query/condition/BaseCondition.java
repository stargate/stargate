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

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterOperationCode;
import io.stargate.sgv2.docsapi.service.util.DocsApiUtils;
import java.util.Optional;
import java.util.function.Predicate;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.tuple.Pair;

/** Interface for the base filtering condition. */
public interface BaseCondition extends Predicate<RowWrapper> {

  /** Returns a {@link BaseCondition} that is the logical negation of this condition. */
  @Override
  BaseCondition negate();

  /**
   * @return If this condition can be executed on the persistence level. The default implementation
   *     resolves to true if the {@link #getBuiltCondition()} returns non-empty value.
   */
  default boolean isPersistenceCondition() {
    return getBuiltCondition().isPresent();
  }

  /**
   * @return Returns persistence built condition together with bind value, if this condition
   *     supports database querying.
   */
  Optional<Pair<BuiltCondition, QueryOuterClass.Value>> getBuiltCondition();

  /** @return Returns filter operation code used by this condition. */
  FilterOperationCode getFilterOperationCode();

  /** @return Returns the query value. */
  Object getQueryValue();

  /** @return Reference to the document properties. */
  DocumentProperties documentProperties();

  /**
   * Returns the most specific value type implied by this condition.
   *
   * <p>Note: if a specific value type is not known, {@code Object.class} should be returned.
   *
   * @return the most specific type of value implied by this condition (must not be null).
   */
  @NotNull
  default Class<?> getQueryValueType() {
    return Object.class;
  }

  /** @return if condition evaluates on the missing fields */
  boolean isEvaluateOnMissingFields();

  /**
   * Resolves {@link String} value from the document {@link RowWrapper}.
   *
   * @param row Row
   * @return Returns resolved value or <code>null</code>
   */
  default String getString(RowWrapper row) {
    return DocsApiUtils.getStringFromRow(row, documentProperties());
  }

  /**
   * Resolves {@link Double} value from the document {@link RowWrapper}.
   *
   * @param row Row
   * @return Returns resolved value or <code>null</code>
   */
  default Double getDouble(RowWrapper row) {
    return DocsApiUtils.getDoubleFromRow(row, documentProperties());
  }

  /**
   * Resolves {@link Boolean} value from the document {@link RowWrapper}.
   *
   * @param row Row
   * @param numericBooleans If <code>true</code> assumes booleans are stored as numeric values.
   * @return Returns resolved value or <code>null</code>
   */
  default Boolean getBoolean(RowWrapper row, boolean numericBooleans) {
    return DocsApiUtils.getBooleanFromRow(row, documentProperties(), numericBooleans);
  }
}
