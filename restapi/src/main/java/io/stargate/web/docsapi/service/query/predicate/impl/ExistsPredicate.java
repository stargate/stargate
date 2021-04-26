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
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import io.stargate.web.docsapi.service.query.predicate.AnyValuePredicate;
import org.immutables.value.Value;

import java.util.Objects;
import java.util.Optional;

/**
 * Exists predicate can resolve if any database value exists.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@Value.Immutable(singleton = true)
public abstract class ExistsPredicate implements AnyValuePredicate<Boolean> {

    public static final String RAW_VALUE = "$exists";

    /**
     * @return Singleton instance
     */
    public static ExistsPredicate of() {
        return ImmutableExistsPredicate.of();
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
        return Optional.of(Predicate.EQ);
    }

    /**
     * Only one database value (string, boolean or double) has to match.
     */
    @Override
    public boolean isMatchAll() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(Boolean filterValue, Boolean dbValue) {
        return null != dbValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(Boolean filterValue, String dbValue) {
        return null != dbValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(Boolean filterValue, Double dbValue) {
        return null != dbValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateBooleanFilterInput(Boolean filterValue) {
        if (!Boolean.TRUE.equals(filterValue)) {
            String msg = String.format("%s only supports the value `true`", RAW_VALUE);
            throw new DocumentAPIRequestException("msg");
        }
    }


}
