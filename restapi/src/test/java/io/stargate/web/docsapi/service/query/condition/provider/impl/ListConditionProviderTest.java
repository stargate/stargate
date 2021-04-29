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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.impl.AnyValueCondition;
import io.stargate.web.docsapi.service.query.filter.operation.CustomValueFilterOperation;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@ExtendWith(MockitoExtension.class)
class ListConditionProviderTest {

    @InjectMocks
    ListConditionProvider provider;

    @Mock
    CustomValueFilterOperation<List<?>> filterOperation;

    @Nested
    class CreateCondition {

        ObjectMapper objectMapper = new ObjectMapper();

        @Test
        public void invalidNode() {
            JsonNode node = objectMapper.createObjectNode();

            Optional<? extends BaseCondition> result = provider.createCondition(node, false);

            assertThat(result).isEmpty();
        }

        @Test
        public void invalidArrayContent() {
            boolean numericBooleans = RandomUtils.nextBoolean();
            ArrayNode node = objectMapper.createArrayNode();
            node.add(objectMapper.createObjectNode());

            Throwable throwable = catchThrowable(() -> provider.createCondition(node, numericBooleans));

            assertThat(throwable).isInstanceOf(DocumentAPIRequestException.class);
        }

        @Test
        public void collectAllSupported() {
            ArrayNode node = objectMapper.createArrayNode()
                    .add(true)
                    .add(23)
                    .add("Jordan")
                    .add((String) null);

            Optional<? extends BaseCondition> result = provider.createCondition(node, false);

            assertThat(result).hasValueSatisfying(baseCondition -> {
                assertThat(baseCondition).isInstanceOfSatisfying(AnyValueCondition.class, bc -> {
                    @SuppressWarnings("unchecked")
                    List<Object> queryValue = (List<Object>) bc.getQueryValue();
                    assertThat(queryValue).hasSize(4)
                            .contains(Boolean.TRUE, 23, "Jordan", null);
                    assertThat(bc.getFilterOperation()).isEqualTo(filterOperation);
                });
            });
        }

    }

}