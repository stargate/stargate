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

package io.stargate.web.docsapi.service.query.predicate;

/**
 * Predicate that can compare values against a {@link Double} database value.
 *
 * @param <I> Type of the filter value to compare to.
 */
public interface DoubleFilterPredicate<I> extends BasePredicate {

    /**
     * Tests the filter value and database against this predicate.
     *
     * @param filterValue Filter value, can be <code>null</code>
     * @param dbValue     DB value, can be <code>null</code>
     * @return <code>true</code> if the filter value matches the predicate, otherwise <code>false</code>
     */
    boolean test(I filterValue, Double dbValue);

    /**
     * Validates if the filter input can be used against this predicate.
     * <p>
     * Implementations should throw proper exception in case the validation fails.
     *
     * @param filterValue Filter value, can be <code>null</code>
     */
    default void validateDoubleFilterInput(I filterValue) {
    };

}
