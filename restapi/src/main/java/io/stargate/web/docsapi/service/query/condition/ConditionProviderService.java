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

package io.stargate.web.docsapi.service.query.condition;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import io.stargate.web.docsapi.service.query.condition.provider.ConditionProvider;
import io.stargate.web.docsapi.service.query.condition.provider.impl.BasicConditionProvider;
import io.stargate.web.docsapi.service.query.condition.provider.impl.ExistsConditionProvider;
import io.stargate.web.docsapi.service.query.condition.provider.impl.ListConditionProvider;
import io.stargate.web.docsapi.service.query.filter.operation.impl.*;

import java.util.*;

/**
 * Simple service that wraps all available raw filter values and connects them to a {@link ConditionProvider}.
 * <p>
 * Currently uses a unmodifiable map, thus it's thread safe.
 */
public class ConditionProviderService {

    /**
     * Map of raw filters to the Condition providers.
     */
    private final Map<String, ConditionProvider> providerMap;

    public ConditionProviderService() {
        this.providerMap = initProviderMap();
    }

    /**
     * Creates the conditions for the node containing the raw filter ops as the keys. For example:
     * <code>{ "$gt: { 5 }, "$lt": { 10 }}</code>.
     * @param conditionsNode Node containing the filter ops as keys
     * @param numericBooleans If numeric boolean should be applied to the created conditions
     * @return Collection of created conditions.
     * @throws io.stargate.web.docsapi.exception.DocumentAPIRequestException If filter op is not found, condition constructions fails or filter value is not supported by the filter op.
     */
    public Collection<BaseCondition> getConditions(JsonNode conditionsNode, boolean numericBooleans) {
        List<BaseCondition> results = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = conditionsNode.fields();
        fields.forEachRemaining(field -> {
            String filterOp = field.getKey();
            ConditionProvider jsonNodeConditionProvider = providerMap.get(filterOp);
            if (null != jsonNodeConditionProvider) {
                JsonNode valueNode = field.getValue();
                Optional<? extends BaseCondition> condition = jsonNodeConditionProvider.createCondition(valueNode, numericBooleans);
                if (condition.isPresent()) {
                    results.add(condition.get());
                } else {
                    // condition
                    String msg = String.format("Operation %s is not supporting the provided value %s.", filterOp, valueNode.toPrettyString());
                    throw new DocumentAPIRequestException(msg);
                }
            } else {
                // provider can not be found
                String msg = String.format("Operation %s is not supported.", filterOp);
                throw new DocumentAPIRequestException(msg);
            }
        });
        return results;
    }

    private static Map<String, ConditionProvider> initProviderMap() {
        Map<String, ConditionProvider> map = new HashMap<>();

        // standard ones, $eq, $lt, lte, $gt, $gte, $ne
        map.put(EqFilterOperation.RAW_VALUE, BasicConditionProvider.of(EqFilterOperation.of()));
        map.put(LtFilterOperation.RAW_VALUE, BasicConditionProvider.of(LtFilterOperation.of()));
        map.put(LteFilterOperation.RAW_VALUE, BasicConditionProvider.of(LteFilterOperation.of()));
        map.put(GtFilterOperation.RAW_VALUE, BasicConditionProvider.of(GtFilterOperation.of()));
        map.put(GteFilterOperation.RAW_VALUE, BasicConditionProvider.of(GteFilterOperation.of()));
        map.put(NeFilterOperation.RAW_VALUE, BasicConditionProvider.of(NeFilterOperation.of()));

        // $in
        map.put(InFilterOperation.RAW_VALUE, ListConditionProvider.of(InFilterOperation.of()));
        map.put(NotInFilterOperation.RAW_VALUE, ListConditionProvider.of(NotInFilterOperation.of()));

        // $exists
        map.put(ExistsFilterOperation.RAW_VALUE, new ExistsConditionProvider());

        return Collections.unmodifiableMap(map);
    }


}
