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

package io.stargate.web.docsapi.service.query.search.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.search.resolver.filter.impl.InMemoryCandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.filter.impl.PersistenceCandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.impl.AllFiltersResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.InMemoryDocumentsResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.PersistenceDocumentsResolver;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SingleExpressionResolverTest {

  @Mock FilterExpression expression;

  @Mock BaseCondition condition;

  @Mock DocumentsResolver candidatesResolver;

  @Nested
  class Resolve {

    @Test
    public void persistenceNoCandidates() {
      ExecutionContext executionContext = ExecutionContext.create(true);
      when(condition.isPersistenceCondition()).thenReturn(true);
      when(expression.getCondition()).thenReturn(condition);

      DocumentsResolver result = SingleExpressionResolver.resolve(expression, executionContext);

      assertThat(result).isInstanceOf(PersistenceDocumentsResolver.class);
    }

    @Test
    public void persistenceCandidates() {
      ExecutionContext executionContext = ExecutionContext.create(true);
      when(condition.isPersistenceCondition()).thenReturn(true);
      when(expression.getCondition()).thenReturn(condition);

      DocumentsResolver result =
          SingleExpressionResolver.resolve(expression, executionContext, candidatesResolver);

      assertThat(result)
          .isInstanceOfSatisfying(
              AllFiltersResolver.class,
              allOf -> {
                assertThat(allOf)
                    .hasFieldOrPropertyWithValue("candidatesResolver", candidatesResolver);
                assertThat(allOf)
                    .extracting("candidatesFilters")
                    .asList()
                    .singleElement()
                    .isInstanceOf(PersistenceCandidatesFilter.class);
              });
    }

    @Test
    public void inMemoryNoCandidates() {
      ExecutionContext executionContext = ExecutionContext.create(true);
      when(condition.isPersistenceCondition()).thenReturn(false);
      when(expression.getCondition()).thenReturn(condition);

      DocumentsResolver result = SingleExpressionResolver.resolve(expression, executionContext);

      assertThat(result).isInstanceOf(InMemoryDocumentsResolver.class);
    }

    @Test
    public void inMemoryCandidates() {
      ExecutionContext executionContext = ExecutionContext.create(true);
      when(condition.isPersistenceCondition()).thenReturn(false);
      when(expression.getCondition()).thenReturn(condition);

      DocumentsResolver result =
          SingleExpressionResolver.resolve(expression, executionContext, candidatesResolver);

      assertThat(result)
          .isInstanceOfSatisfying(
              AllFiltersResolver.class,
              allOf -> {
                assertThat(allOf)
                    .hasFieldOrPropertyWithValue("candidatesResolver", candidatesResolver);
                assertThat(allOf)
                    .extracting("candidatesFilters")
                    .asList()
                    .singleElement()
                    .isInstanceOf(InMemoryCandidatesFilter.class);
              });
    }
  }
}
