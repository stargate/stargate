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
import static org.assertj.core.api.Assertions.catchThrowable;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Literal;
import com.bpodgursky.jbool_expressions.Or;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.ImmutableFilterExpression;
import io.stargate.web.docsapi.service.query.ImmutableFilterPath;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.web.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.GtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.NeFilterOperation;
import io.stargate.web.docsapi.service.query.search.resolver.filter.impl.InMemoryCandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.filter.impl.PersistenceCandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.impl.AllFiltersResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.InMemoryDocumentsResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.PersistenceDocumentsResolver;
import java.util.Collections;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BaseResolverTest {

  @Mock DocumentsResolver candidatesResolver;

  @Nested
  class Resolve {

    @Test
    public void literalTrue() {
      ExecutionContext context = ExecutionContext.create(true);

      DocumentsResolver result = BaseResolver.resolve(Literal.getTrue(), context);

      assertThat(result).isNull();
    }

    @Test
    public void singleExpressionPersistenceNoCandidates() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      DocumentsResolver result = BaseResolver.resolve(expression, context);

      assertThat(result).isInstanceOf(PersistenceDocumentsResolver.class);
    }

    @Test
    public void singleExpressionPersistenceCandidates() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition = ImmutableStringCondition.of(EqFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      DocumentsResolver result = BaseResolver.resolve(expression, context, candidatesResolver);

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
    public void singleExpressionInMemoryNoCandidates() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition = ImmutableStringCondition.of(NeFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      DocumentsResolver result = BaseResolver.resolve(expression, context);

      assertThat(result).isInstanceOf(InMemoryDocumentsResolver.class);
    }

    @Test
    public void singleExpressionInMemoryCandidates() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition = ImmutableStringCondition.of(NeFilterOperation.of(), "find-me");
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      DocumentsResolver result = BaseResolver.resolve(expression, context, candidatesResolver);

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

    @Test
    public void andOnSamePath() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition1 = ImmutableStringCondition.of(GtFilterOperation.of(), "find-me");
      BaseCondition condition2 = ImmutableStringCondition.of(LtFilterOperation.of(), "find-me");
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath, condition1, 0);
      FilterExpression expression2 = ImmutableFilterExpression.of(filterPath, condition2, 1);

      DocumentsResolver result = BaseResolver.resolve(And.of(expression1, expression2), context);

      assertThat(result).isInstanceOf(PersistenceDocumentsResolver.class);
    }

    @Test
    public void andWithSingleExpressionDoesNotFail() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition1 = ImmutableStringCondition.of(GtFilterOperation.of(), "find-me");
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath, condition1, 0);

      DocumentsResolver result = BaseResolver.resolve(And.of(expression1), context);

      assertThat(result).isInstanceOf(PersistenceDocumentsResolver.class);
    }

    @Test
    public void orOnSamePath() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition1 = ImmutableStringCondition.of(GtFilterOperation.of(), "find-me");
      BaseCondition condition2 = ImmutableStringCondition.of(LtFilterOperation.of(), "find-me");
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath, condition1, 0);
      FilterExpression expression2 = ImmutableFilterExpression.of(filterPath, condition2, 1);

      Throwable t =
          catchThrowable(() -> BaseResolver.resolve(Or.of(expression1, expression2), context));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_SEARCH_OR_NOT_SUPPORTED);
    }
  }
}
