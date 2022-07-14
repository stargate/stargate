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
 * Filter operation that can compare {@link String}, {@link Double} and {@link Boolean} database
 * values against their corresponding {@link String}, {@link Number} and {@link Boolean} filter
 * values.
 */
public interface ValueFilterOperation extends BaseFilterOperation {

  /**
   * Tests the filter {@link String} value and database {@link String} against this filter
   * operation.
   *
   * @param dbValue DB value, can be <code>null</code>
   * @param filterValue Filter value, can be <code>null</code>
   * @return <code>true</code> if the filter value matches the filter, otherwise <code>false
   * </code>
   */
  boolean test(String dbValue, String filterValue);

  /**
   * Validates if the filter input can be used against this filter operation.
   *
   * <p>Implementations should throw proper exception in case the validation fails.
   *
   * @param filterValue Filter value, can be <code>null</code>
   */
  default void validateStringFilterInput(String filterValue) {
    // default impl empty
  }

  /**
   * Tests the filter {@link Number} value and database {@link Double} against this filter
   * operation.
   *
   * @param dbValue DB value, can be <code>null</code>
   * @param filterValue Filter value, can be <code>null</code>
   * @return <code>true</code> if the filter value matches the filter, otherwise <code>false
   * </code>
   */
  boolean test(Double dbValue, Number filterValue);

  /**
   * Validates if the filter input can be used against this filter operation.
   *
   * <p>Implementations should throw proper exception in case the validation fails.
   *
   * @param filterValue Filter value, can be <code>null</code>
   */
  default void validateNumberFilterInput(Number filterValue) {
    // default impl empty
  }

  /**
   * Tests the filter {@link Boolean} value and database {@link Boolean} against this filter
   * operation.
   *
   * @param dbValue DB value, can be <code>null</code>
   * @param filterValue Filter value, can be <code>null</code>
   * @return <code>true</code> if the filter value matches the filter, otherwise <code>false
   * </code>
   */
  boolean test(Boolean dbValue, Boolean filterValue);

  /**
   * Validates if the filter input can be used against this filter operation.
   *
   * <p>Implementations should throw proper exception in case the validation fails.
   *
   * @param filterValue Filter value, can be <code>null</code>
   */
  default void validateBooleanFilterInput(Boolean filterValue) {
    // default impl empty
  }

  @Override
  ValueFilterOperation negate();
}
