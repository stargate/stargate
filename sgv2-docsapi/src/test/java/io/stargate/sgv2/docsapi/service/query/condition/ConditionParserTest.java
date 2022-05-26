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

package io.stargate.sgv2.docsapi.service.query.condition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableExistsCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableGenericCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.GtFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.GteFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.InFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.LteFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.NeFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.NotInFilterOperation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ConditionParserTest {

  // false with the default implementation
  private static final boolean NUMERIC_BOOLEANS = false;

  @Inject ConditionParser conditionParser;

  @Inject ObjectMapper objectMapper;

  @Inject DocumentProperties documentProperties;

  @Nested
  class GetConditions {

    @Test
    public void invalidNode() {
      JsonNode node = objectMapper.missingNode();

      Throwable throwable =
          catchThrowable(() -> conditionParser.getConditions(node, NUMERIC_BOOLEANS));

      assertThat(throwable)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_SEARCH_FILTER_INVALID);
    }

    @Test
    public void invalidOps() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$exgf", value);

      Throwable throwable =
          catchThrowable(() -> conditionParser.getConditions(node, NUMERIC_BOOLEANS));

      assertThat(throwable)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_SEARCH_FILTER_INVALID);
    }

    @Test
    public void invalidValue() {
      // simulate with in with wrong value
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$in", value);

      Throwable throwable =
          catchThrowable(() -> conditionParser.getConditions(node, NUMERIC_BOOLEANS));

      assertThat(throwable)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_SEARCH_FILTER_INVALID);
    }

    @Test
    public void eq() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$eq", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node, NUMERIC_BOOLEANS);

      Assertions.assertThat(conditions)
          .containsOnly(
              ImmutableStringCondition.of(EqFilterOperation.of(), value, documentProperties));
    }

    @Test
    public void ne() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$ne", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node, NUMERIC_BOOLEANS);

      Assertions.assertThat(conditions)
          .containsOnly(
              ImmutableStringCondition.of(NeFilterOperation.of(), value, documentProperties));
    }

    @Test
    public void gt() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$gt", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node, NUMERIC_BOOLEANS);

      Assertions.assertThat(conditions)
          .containsOnly(
              ImmutableStringCondition.of(GtFilterOperation.of(), value, documentProperties));
    }

    @Test
    public void gte() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$gte", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node, NUMERIC_BOOLEANS);

      Assertions.assertThat(conditions)
          .containsOnly(
              ImmutableStringCondition.of(GteFilterOperation.of(), value, documentProperties));
    }

    @Test
    public void lt() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$lt", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node, NUMERIC_BOOLEANS);

      Assertions.assertThat(conditions)
          .containsOnly(
              ImmutableStringCondition.of(LtFilterOperation.of(), value, documentProperties));
    }

    @Test
    public void lte() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      ObjectNode node = objectMapper.createObjectNode().put("$lte", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node, NUMERIC_BOOLEANS);

      Assertions.assertThat(conditions)
          .containsOnly(
              ImmutableStringCondition.of(LteFilterOperation.of(), value, documentProperties));
    }

    @Test
    public void exists() {
      ObjectNode node = objectMapper.createObjectNode().put("$exists", true);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node, NUMERIC_BOOLEANS);

      Assertions.assertThat(conditions)
          .containsOnly(ImmutableExistsCondition.of(true, documentProperties));
    }

    @Test
    public void in() {
      ArrayNode value = objectMapper.createArrayNode().add(2);
      ObjectNode node = objectMapper.createObjectNode().set("$in", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node, NUMERIC_BOOLEANS);

      List<Object> expected = new ArrayList<>();
      expected.add(2);
      Assertions.assertThat(conditions)
          .containsOnly(
              ImmutableGenericCondition.of(
                  InFilterOperation.of(), expected, documentProperties, NUMERIC_BOOLEANS));
    }

    @Test
    public void nin() {
      ArrayNode value = objectMapper.createArrayNode().add(2);
      ObjectNode node = objectMapper.createObjectNode().set("$nin", value);

      Collection<BaseCondition> conditions = conditionParser.getConditions(node, NUMERIC_BOOLEANS);

      List<Object> expected = new ArrayList<>();
      expected.add(2);
      Assertions.assertThat(conditions)
          .containsOnly(
              ImmutableGenericCondition.of(
                  NotInFilterOperation.of(), expected, documentProperties, NUMERIC_BOOLEANS));
    }
  }
}
