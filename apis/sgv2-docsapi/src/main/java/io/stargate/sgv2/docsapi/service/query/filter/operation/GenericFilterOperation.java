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

package io.stargate.sgv2.docsapi.service.query.filter.operation;

/**
 * Special type of filter operation that can test a generic input value against all database values.
 *
 * @param <FV> Type of the filter value used in the filter.
 */
public interface GenericFilterOperation<FV> extends BaseFilterOperation {

  /**
   * Tests the filter value and database {@link String} against this filter operation.
   *
   * @param dbValue DB value, can be <code>null</code>
   * @param filterValue Filter value, can be <code>null</code>
   * @return <code>true</code> if the filter value matches the filter, otherwise <code>false
   * </code>
   */
  boolean test(String dbValue, FV filterValue);

  /**
   * Tests the filter value and database {@link Double} against this filter operation.
   *
   * @param dbValue DB value, can be <code>null</code>
   * @param filterValue Filter value, can be <code>null</code>
   * @return <code>true</code> if the filter value matches the filter, otherwise <code>false
   * </code>
   */
  boolean test(Double dbValue, FV filterValue);

  /**
   * Tests the filter value and database {@link Boolean} against this filter operation.
   *
   * @param dbValue DB value, can be <code>null</code>
   * @param filterValue Filter value, can be <code>null</code>
   * @return <code>true</code> if the filter value matches the filter, otherwise <code>false
   * </code>
   */
  boolean test(Boolean dbValue, FV filterValue);

  /**
   * Validates if the filter input can be used against this filter operation.
   *
   * <p>Implementations should throw proper exception in case the validation fails.
   *
   * @param filterValue Filter value, can be <code>null</code>
   */
  default void validateFilterInput(FV filterValue) {
    // default impl empty
  }

  GenericFilterOperation<FV> negate();
}
