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
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.predicate.AnyValuePredicate;
import org.immutables.value.Value;

import java.util.List;
import java.util.Optional;

@Value.Immutable
public abstract class ListCondition implements BaseCondition {

    /**
     * @return Predicate for the condition.
     */
    @Value.Parameter
    public abstract AnyValuePredicate<List<?>> getFilterPredicate();

    /**
     * @return Filter query value.
     */
    @Value.Parameter
    public abstract List<?> getQueryValue();

    /**
     * @return If booleans should be considered as numeric values.
     */
    @Value.Parameter
    public abstract boolean isNumericBooleans();

    /**
     * Validates the value against the predicate.
     */
    @Value.Check
    protected void validate() {
        getFilterPredicate().validateBooleanFilterInput(getQueryValue());
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
        // TODO support for all match or any match
        AnyValuePredicate<List<?>> filterPredicate = getFilterPredicate();
        List<?> queryValue = getQueryValue();

        // test boolean, then number, then string
        Boolean dbValueBoolean = getBooleanDatabaseValue(row, isNumericBooleans());
        if (null != dbValueBoolean) {
            return filterPredicate.test(queryValue, dbValueBoolean);
        }

        Double dbValueDouble = getDoubleDatabaseValue(row);
        if (null != dbValueDouble) {
            return filterPredicate.test(queryValue, dbValueDouble);
        }

        String dbValueString = getStringDatabaseValue(row);
        return filterPredicate.test(queryValue, dbValueString);
    }

}
