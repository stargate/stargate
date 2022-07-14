/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.service.query.search.weight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ExpressionWeightResolverTest {

  ExpressionWeightResolver<FilterExpression> weightResolver = new ExpressionWeightResolver<>() {};

  @Mock FilterExpression expression1;

  @Mock FilterExpression expression2;

  @Mock BaseCondition baseCondition1;

  @Mock BaseCondition baseCondition2;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(expression1.getCondition()).thenReturn(baseCondition1);
    when(expression2.getCondition()).thenReturn(baseCondition2);
  }

  @Nested
  class Single {

    @Test
    public void persistenceWins() {
      when(baseCondition1.isPersistenceCondition()).thenReturn(true);
      when(baseCondition2.isPersistenceCondition()).thenReturn(false);

      FilterExpression result = weightResolver.single().apply(expression1, expression2);
      FilterExpression resultReverse = weightResolver.single().apply(expression2, expression1);

      assertThat(result).isEqualTo(expression1);
      assertThat(resultReverse).isEqualTo(expression1);
    }

    @Test
    public void nonEvalOnMissingWins() {
      when(baseCondition1.isPersistenceCondition()).thenReturn(false);
      when(baseCondition1.isEvaluateOnMissingFields()).thenReturn(false);
      when(baseCondition2.isPersistenceCondition()).thenReturn(false);
      when(baseCondition2.isEvaluateOnMissingFields()).thenReturn(true);

      FilterExpression result = weightResolver.single().apply(expression1, expression2);
      FilterExpression resultReverse = weightResolver.single().apply(expression2, expression1);

      assertThat(result).isEqualTo(expression1);
      assertThat(resultReverse).isEqualTo(expression1);
    }
  }

  @Nested
  class CollectionCompare {

    @BeforeEach
    public void setup() {
      when(expression1.getCondition()).thenReturn(baseCondition1);
      when(expression2.getCondition()).thenReturn(baseCondition2);
    }

    @Test
    public void singlePersistenceWinOverMixed() {
      when(baseCondition1.isPersistenceCondition()).thenReturn(true);
      when(baseCondition2.isPersistenceCondition()).thenReturn(false);
      List<FilterExpression> first = Arrays.asList(expression1, expression1, expression2);
      List<FilterExpression> second = Collections.singletonList(expression1);

      Collection<FilterExpression> result = weightResolver.collection().apply(first, second);
      Collection<FilterExpression> resultReversed =
          weightResolver.collection().apply(second, first);

      assertThat(result).isEqualTo(second);
      assertThat(resultReversed).isEqualTo(second);
    }

    @Test
    public void multiPersistenceWinOverMixed() {
      when(baseCondition1.isPersistenceCondition()).thenReturn(true);
      when(baseCondition2.isPersistenceCondition()).thenReturn(false);
      List<FilterExpression> first = Arrays.asList(expression1, expression1, expression2);
      List<FilterExpression> second = Arrays.asList(expression1, expression1);

      Collection<FilterExpression> result = weightResolver.collection().apply(first, second);
      Collection<FilterExpression> resultReversed =
          weightResolver.collection().apply(second, first);

      assertThat(result).isEqualTo(second);
      assertThat(resultReversed).isEqualTo(second);
    }

    @Test
    public void inMemoryWinsOverEval() {
      when(baseCondition1.isPersistenceCondition()).thenReturn(false);
      when(baseCondition2.isPersistenceCondition()).thenReturn(false);
      when(baseCondition2.isEvaluateOnMissingFields()).thenReturn(true);
      List<FilterExpression> first = Arrays.asList(expression1, expression2);
      List<FilterExpression> second = Arrays.asList(expression1, expression1);

      Collection<FilterExpression> result = weightResolver.collection().apply(first, second);
      Collection<FilterExpression> resultReversed =
          weightResolver.collection().apply(second, first);

      assertThat(result).isEqualTo(second);
      assertThat(resultReversed).isEqualTo(second);
    }
  }
}
