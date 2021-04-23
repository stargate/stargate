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
import io.stargate.web.docsapi.service.query.predicate.impl.*;

import java.util.*;

public class ConditionProviderService {

    private final Map<String, ConditionProvider> providerMap;

    public ConditionProviderService() {
        this.providerMap = initProviderMap();
    }

    public Collection<BaseCondition> getConditions(JsonNode conditionsNode, boolean numericBooleans) {
        List<BaseCondition> results = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = conditionsNode.fields();
        fields.forEachRemaining(field -> {
            String filterOp = field.getKey();
            ConditionProvider jsonNodeConditionProvider = providerMap.get(filterOp);
            if (null != jsonNodeConditionProvider) {
                Optional<? extends BaseCondition> condition = jsonNodeConditionProvider.createCondition(field.getValue(), numericBooleans);
                condition.ifPresent(results::add);
            }

            // TODO what in case fo the non found, f.e $my, throw exception?
        });
        return results;
    }

    private static Map<String, ConditionProvider> initProviderMap() {
        Map<String, ConditionProvider> map = new HashMap<>();

        // standard ones, $eq, $lt, lte, $gt, $gte, $ne
        map.put(EqPredicate.RAW_VALUE, ImmutableBasicConditionProvider.of(EqPredicate.of()));
        map.put(LtPredicate.RAW_VALUE, ImmutableBasicConditionProvider.of(LtPredicate.of()));
        map.put(LtePredicate.RAW_VALUE, ImmutableBasicConditionProvider.of(LtePredicate.of()));
        map.put(GtPredicate.RAW_VALUE, ImmutableBasicConditionProvider.of(GtPredicate.of()));
        map.put(GtePredicate.RAW_VALUE, ImmutableBasicConditionProvider.of(GtePredicate.of()));
        map.put(NePredicate.RAW_VALUE, ImmutableBasicConditionProvider.of(NePredicate.of()));

        // $in
        map.put(InPredicate.RAW_VALUE, ImmutableListConditionProvider.of(InPredicate.of()));
        map.put(NotInPredicate.RAW_VALUE, ImmutableListConditionProvider.of(NotInPredicate.of()));

        // $exists
        map.put(ExistsPredicate.RAW_VALUE, new ExistsConditionProvider());

        return Collections.unmodifiableMap(map);
    }


}
