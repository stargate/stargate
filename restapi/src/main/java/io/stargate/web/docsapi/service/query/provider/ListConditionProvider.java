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

package io.stargate.web.docsapi.service.query.provider;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.impl.AnyValueCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableAnyValueCondition;
import io.stargate.web.docsapi.service.query.predicate.AnyValuePredicate;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Condition provider for the {@link ListCondition}. Extract objects from the array JSON node.
 */
@Value.Immutable
public abstract class ListConditionProvider implements ConditionProvider {

    /**
     * @return Predicate to use in the condition
     */
    @Value.Parameter
    protected abstract AnyValuePredicate<List<?>> listPredicate();

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<? extends BaseCondition> createCondition(JsonNode node, boolean numericBooleans) {
        if (node.isArray()) {
            Iterator<JsonNode> iterator = node.iterator();
            List<?> input = getListConditionValues(iterator);
            AnyValueCondition<List<?>> condition = ImmutableAnyValueCondition.of(listPredicate(), input, numericBooleans);
            return Optional.of(condition);
        }
        return Optional.empty();
    }

    private List<?> getListConditionValues(Iterator<JsonNode> iterator) {
        List<Object> result = new ArrayList<>();
        iterator.forEachRemaining(node -> {
            // TODO Eric? copied int branch from the Document service, is this actually wanted?

            if (node.isInt()) {
                result.add(node.asInt());
            } else if (node.isBoolean()) {
                result.add(node.asBoolean());
            } else if (node.isNumber()) {
                result.add(node.asDouble());
            } else if (node.isTextual()) {
                result.add(node.asText());
            } else if (node.isNull()) {
                result.add(null);
            }

            // TODO should we throw the exception here if node is smth else?
            //  for example $in: [ 1, [2]] would ignore 2 here
        });
        return result;
    }

}
