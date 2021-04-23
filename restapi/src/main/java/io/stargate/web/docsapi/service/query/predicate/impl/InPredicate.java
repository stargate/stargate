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

package io.stargate.web.docsapi.service.query.predicate.impl;

import io.stargate.db.query.Predicate;
import io.stargate.web.docsapi.service.query.predicate.AnyValuePredicate;
import org.immutables.value.Value;

import java.util.List;
import java.util.Optional;

/**
 * In list predicate.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@Value.Immutable(singleton = true)
public abstract class InPredicate implements AnyValuePredicate<List<?>> {

    public static final String RAW_VALUE = "$in";

    /**
     * @return Singleton instance
     */
    public static InPredicate of() {
        return ImmutableInPredicate.of();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getRawValue() {
        return RAW_VALUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Predicate> getDatabasePredicate() {
        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(List<?> filterValue, String dbValue) {
        return filterValue.stream()
                .filter(String.class::isInstance)
                .anyMatch(value -> value.equals(dbValue));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(List<?> filterValue, Boolean dbValue) {
        return filterValue.stream()
                .filter(Boolean.class::isInstance)
                .anyMatch(value -> value.equals(dbValue));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(List<?> filterValue, Double dbValue) {
        // TODO maybe more correct number matching here as well
        return filterValue.stream()
                .filter(Number.class::isInstance)
                .map(Number.class::cast)
                .anyMatch(value -> Double.valueOf(value.doubleValue()).equals(dbValue));
    }

    // TODO empty list validation needed?

}
