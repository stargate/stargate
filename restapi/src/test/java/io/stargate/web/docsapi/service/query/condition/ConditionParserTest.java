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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableCombinedCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableExistsCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.web.docsapi.service.query.filter.operation.impl.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ConditionParserTest {

  private static final boolean NUMERIC_BOOLEANS = RandomUtils.nextBoolean();

  ConditionParser conditionParser = new ConditionParser(NUMERIC_BOOLEANS);

  @Nested
  class GetConditions {

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void invalidOps() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$exgf", value);

      Throwable throwable =
          catchThrowable(() -> conditionParser.getConditions(node));

      assertThat(throwable).isInstanceOf(DocumentAPIRequestException.class);
    }

    @Test
    public void invalidValue() {
      // simulate with in with wrong value
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$in", value);

      Throwable throwable =
          catchThrowable(() -> conditionParser.getConditions(node));

      assertThat(throwable).isInstanceOf(DocumentAPIRequestException.class);
    }

    @Test
    public void eq() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$eq", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node);

      assertThat(conditions)
          .containsOnly(ImmutableStringCondition.of(EqFilterOperation.of(), value));
    }

    @Test
    public void ne() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$ne", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node);

      assertThat(conditions)
          .containsOnly(ImmutableStringCondition.of(NeFilterOperation.of(), value));
    }

    @Test
    public void gt() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$gt", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node);

      assertThat(conditions)
          .containsOnly(ImmutableStringCondition.of(GtFilterOperation.of(), value));
    }

    @Test
    public void gte() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$gte", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node);

      assertThat(conditions)
          .containsOnly(ImmutableStringCondition.of(GteFilterOperation.of(), value));
    }

    @Test
    public void lt() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$lt", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node);

      assertThat(conditions)
          .containsOnly(ImmutableStringCondition.of(LtFilterOperation.of(), value));
    }

    @Test
    public void lte() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$lte", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node);

      assertThat(conditions)
          .containsOnly(ImmutableStringCondition.of(LteFilterOperation.of(), value));
    }

    @Test
    public void exists() {
      ObjectNode node = objectMapper.createObjectNode().put("$exists", true);

      Collection<BaseCondition> conditions =
          conditionParser.getConditions(node);

      assertThat(conditions)
          .containsOnly(
              ImmutableExistsCondition.of(ExistsFilterOperation.of(), true, NUMERIC_BOOLEANS));
    }

    @Test
    public void in() {
      ArrayNode value = objectMapper.createArrayNode().add(2);
      ObjectNode node = objectMapper.createObjectNode().set("$in", value);

      Collection<BaseCondition> conditions =
          conditionParser.getConditions(node);

      List<Object> expected = new ArrayList<>();
      expected.add(2);
      assertThat(conditions)
          .containsOnly(
              ImmutableCombinedCondition.of(InFilterOperation.of(), expected, NUMERIC_BOOLEANS));
    }

    @Test
    public void nin() {
      ArrayNode value = objectMapper.createArrayNode().add(2);
      ObjectNode node = objectMapper.createObjectNode().set("$nin", value);

      Collection<BaseCondition> conditions =
          conditionParser.getConditions(node);

      List<Object> expected = new ArrayList<>();
      expected.add(2);
      assertThat(conditions)
          .containsOnly(
              ImmutableCombinedCondition.of(NotInFilterOperation.of(), expected, NUMERIC_BOOLEANS));
    }
  }
}
