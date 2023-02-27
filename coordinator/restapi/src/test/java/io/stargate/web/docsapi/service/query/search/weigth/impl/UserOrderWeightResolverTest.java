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

package io.stargate.web.docsapi.service.query.search.weigth.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UserOrderWeightResolverTest {

  UserOrderWeightResolver resolver = UserOrderWeightResolver.of();

  @Nested
  class Compare {

    @Mock FilterExpression e1;

    @Mock FilterExpression e2;

    @Mock FilterExpression e3;

    @Mock BaseCondition condition1;

    @Mock BaseCondition condition2;

    @Mock BaseCondition condition3;

    @BeforeEach
    public void init() {
      lenient().when(e1.getCondition()).thenReturn(condition1);
      lenient().when(e2.getCondition()).thenReturn(condition2);
      lenient().when(e3.getCondition()).thenReturn(condition3);
    }

    @Test
    public void singleHappyPath() {
      when(e1.getOrderIndex()).thenReturn(0);
      when(e2.getOrderIndex()).thenReturn(1);

      FilterExpression result = resolver.single().apply(e1, e2);
      FilterExpression resultReversed = resolver.single().apply(e2, e1);

      assertThat(result).isEqualTo(e1);
      assertThat(resultReversed).isEqualTo(e1);
    }

    @Test
    public void singleSuperRespected() {
      when(condition2.isPersistenceCondition()).thenReturn(true);

      FilterExpression result = resolver.single().apply(e1, e2);
      FilterExpression resultReversed = resolver.single().apply(e2, e1);

      assertThat(result).isEqualTo(e2);
      assertThat(resultReversed).isEqualTo(e2);
    }

    @Test
    public void collectionHappyPath() {
      when(e1.getOrderIndex()).thenReturn(0);
      when(e2.getOrderIndex()).thenReturn(1);
      when(e3.getOrderIndex()).thenReturn(2);

      List<FilterExpression> c1 = Arrays.asList(e1, e2);
      List<FilterExpression> c2 = Arrays.asList(e2, e3);

      Collection<FilterExpression> result = resolver.collection().apply(c1, c2);
      Collection<FilterExpression> resultReversed = resolver.collection().apply(c2, c1);

      assertThat(result).isEqualTo(c1);
      assertThat(resultReversed).isEqualTo(c1);
    }

    @Test
    public void collectionSuperRespected() {
      when(condition3.isPersistenceCondition()).thenReturn(true);

      List<FilterExpression> c1 = Arrays.asList(e1, e2);
      List<FilterExpression> c2 = Arrays.asList(e2, e3);

      Collection<FilterExpression> result = resolver.collection().apply(c1, c2);
      Collection<FilterExpression> resultReversed = resolver.collection().apply(c2, c1);

      assertThat(result).isEqualTo(c2);
      assertThat(resultReversed).isEqualTo(c2);
    }
  }

  @Nested
  class CompareWithSelectivity {

    @Mock FilterExpression e1;

    @Mock FilterExpression e2;

    @Mock FilterExpression e3;

    @Mock FilterExpression e4;

    @Mock BaseCondition condition;

    @BeforeEach
    public void init() {
      lenient().when(e1.getCondition()).thenReturn(condition);
      lenient().when(e2.getCondition()).thenReturn(condition);
      lenient().when(e3.getCondition()).thenReturn(condition);
      lenient().when(e4.getCondition()).thenReturn(condition);
    }

    @Test
    public void singleExplicitSelectivity() {
      when(e1.getSelectivity()).thenReturn(1.0);
      when(e2.getSelectivity()).thenReturn(0.9);

      // Note: e2 has smaller (better) selectivity
      assertThat(resolver.single().apply(e1, e2)).isEqualTo(e2);
      assertThat(resolver.single().apply(e2, e1)).isEqualTo(e2);
    }

    @Test
    public void collectionExplicitSelectivityUnambiguous() {
      when(e1.getSelectivity()).thenReturn(1.0); // worst selectivity
      when(e2.getSelectivity()).thenReturn(0.5);
      when(e3.getSelectivity()).thenReturn(0.1); // best selectivity

      List<FilterExpression> c1 = Arrays.asList(e1, e2);
      List<FilterExpression> c2 = Arrays.asList(e2, e3);

      // Note: each c1 element is not better that each c2 element
      assertThat(resolver.collection().apply(c1, c2)).isEqualTo(c2);
      assertThat(resolver.collection().apply(c2, c1)).isEqualTo(c2);

      List<FilterExpression> c3 = Arrays.asList(e1, e3);

      // Note: c1 elements are worse than c2 elements "in total"
      assertThat(resolver.collection().apply(c1, c3)).isEqualTo(c3);
      assertThat(resolver.collection().apply(c3, c1)).isEqualTo(c3);

      // Note: c3 elements are worse than c2 elements "in total"
      assertThat(resolver.collection().apply(c2, c3)).isEqualTo(c2);
      assertThat(resolver.collection().apply(c3, c2)).isEqualTo(c2);
    }

    @Test
    public void collectionExplicitSelectivityComplex() {
      when(e1.getSelectivity()).thenReturn(1.0); // worst selectivity
      when(e2.getSelectivity()).thenReturn(0.5);
      when(e3.getSelectivity()).thenReturn(0.2);
      when(e4.getSelectivity()).thenReturn(0.1); // best selectivity

      // Note: c1 elements are equal to c2 elements "in total"
      List<FilterExpression> c1 = Arrays.asList(e1, e4);
      List<FilterExpression> c2 = Arrays.asList(e2, e3);

      // c1 contains the best selectivity filter, so it wins
      assertThat(resolver.collection().apply(c1, c2)).isEqualTo(c1);
      assertThat(resolver.collection().apply(c2, c1)).isEqualTo(c1);
    }
  }
}
