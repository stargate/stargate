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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.impl.BooleanCondition;
import io.stargate.web.docsapi.service.query.condition.impl.NumberCondition;
import io.stargate.web.docsapi.service.query.condition.impl.StringCondition;
import io.stargate.web.docsapi.service.query.filter.operation.impl.NotNullValueFilterOperation;
import java.util.Optional;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BasicConditionProviderTest {

  @InjectMocks BasicConditionProvider<?> provider;

  @Mock NotNullValueFilterOperation filterOperation;

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
    public void booleanNode() {
      boolean numericBooleans = RandomUtils.nextBoolean();
      JsonNode node = objectMapper.createObjectNode().put("some", true).get("some");

      Optional<? extends BaseCondition> result = provider.createCondition(node, numericBooleans);

      assertThat(result)
          .hasValueSatisfying(
              baseCondition -> {
                assertThat(baseCondition)
                    .isInstanceOfSatisfying(
                        BooleanCondition.class,
                        bc -> {
                          assertThat(bc.getQueryValue()).isEqualTo(true);
                          assertThat(bc.isNumericBooleans()).isEqualTo(numericBooleans);
                          assertThat(bc.getFilterOperation()).isEqualTo(filterOperation);
                        });
              });
    }

    @Test
    public void stringNode() {
      String value = "value";
      JsonNode node = objectMapper.createObjectNode().put("some", value).get("some");

      Optional<? extends BaseCondition> result = provider.createCondition(node, false);

      assertThat(result)
          .hasValueSatisfying(
              baseCondition -> {
                assertThat(baseCondition)
                    .isInstanceOfSatisfying(
                        StringCondition.class,
                        bc -> {
                          assertThat(bc.getQueryValue()).isEqualTo(value);
                          assertThat(bc.getFilterOperation()).isEqualTo(filterOperation);
                        });
              });
    }

    @Test
    public void numberNode() {
      int value = 23;
      JsonNode node = objectMapper.createObjectNode().put("some", value).get("some");

      Optional<? extends BaseCondition> result = provider.createCondition(node, false);

      assertThat(result)
          .hasValueSatisfying(
              baseCondition -> {
                assertThat(baseCondition)
                    .isInstanceOfSatisfying(
                        NumberCondition.class,
                        bc -> {
                          assertThat(bc.getQueryValue()).isEqualTo(value);
                          assertThat(bc.getFilterOperation()).isEqualTo(filterOperation);
                        });
              });
    }
  }
}
