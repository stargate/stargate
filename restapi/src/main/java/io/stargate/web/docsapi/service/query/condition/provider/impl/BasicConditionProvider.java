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

package io.stargate.web.docsapi.service.query.condition.provider.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.impl.*;
import io.stargate.web.docsapi.service.query.condition.provider.ConditionProvider;
import io.stargate.web.docsapi.service.query.predicate.BooleanValuePredicate;
import io.stargate.web.docsapi.service.query.predicate.DoubleFilterPredicate;
import io.stargate.web.docsapi.service.query.predicate.StringFilterPredicate;
import org.immutables.value.Value;

import java.util.Optional;

/**
 * The basic {@link ConditionProvider} that usually handles all standard predicates that works will String, Boolean and Number filter value.
 *
 * @param <V> Type of the predicate
 */
public class BasicConditionProvider<V extends StringFilterPredicate<String> & DoubleFilterPredicate<Number> & BooleanValuePredicate<Boolean>> implements ConditionProvider {

    public static <V extends StringFilterPredicate<String> & DoubleFilterPredicate<Number> & BooleanValuePredicate<Boolean>> BasicConditionProvider<V> of(V predicate) {
        return new BasicConditionProvider<>(predicate);
    }

    /**
     * Predicate to use when creating the condition.
     */
    private final V predicate;

    public BasicConditionProvider(V predicate) {
        this.predicate = predicate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<? extends BaseCondition> createCondition(JsonNode node, boolean numericBooleans) {
        if (node.isNumber()) {
            NumberCondition condition = ImmutableNumberCondition.of(predicate, node.numberValue());
            return Optional.of(condition);
        } else if (node.isBoolean()) {
            BooleanCondition condition = ImmutableBooleanCondition.of(predicate, node.asBoolean(), numericBooleans);
            return Optional.of(condition);
        } else if (node.isTextual()) {
            StringCondition condition = ImmutableStringCondition.of(predicate, node.asText());
            return Optional.of(condition);
        }
        return Optional.empty();
    }

}
