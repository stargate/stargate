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

package io.stargate.sgv2.docsapi.service.query.search.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Literal;
import com.bpodgursky.jbool_expressions.Not;
import com.bpodgursky.jbool_expressions.Or;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.FilterPath;
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterExpression;
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterPath;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterOperationCode;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.GtFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.NeFilterOperation;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.FilterExpressionSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.resolver.filter.impl.InMemoryCandidatesFilter;
import io.stargate.sgv2.docsapi.service.query.search.resolver.filter.impl.PersistenceCandidatesFilter;
import io.stargate.sgv2.docsapi.service.query.search.resolver.impl.AllFiltersResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.impl.InMemoryDocumentsResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.impl.OrExpressionDocumentsResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.impl.PersistenceDocumentsResolver;
import java.util.Collection;
import java.util.Collections;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class BaseResolverTest {

  @Mock DocumentsResolver candidatesResolver;

  @Inject DocumentProperties documentProperties;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
  }

  @Nested
  class Resolve {

    @Test
    public void literalTrue() {
      ExecutionContext context = ExecutionContext.create(true);

      DocumentsResolver result =
          BaseResolver.resolve(Literal.getTrue(), context, documentProperties);

      assertThat(result).isNull();
    }

    @Test
    public void singleExpressionPersistenceNoCandidates() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(EqFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      DocumentsResolver result = BaseResolver.resolve(expression, context, documentProperties);

      assertThat(result).isInstanceOf(PersistenceDocumentsResolver.class);
    }

    @Test
    public void singleExpressionPersistenceCandidates() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(EqFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      DocumentsResolver result =
          BaseResolver.resolve(expression, context, candidatesResolver, documentProperties);

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
      BaseCondition condition =
          ImmutableStringCondition.of(NeFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      DocumentsResolver result = BaseResolver.resolve(expression, context, documentProperties);

      assertThat(result).isInstanceOf(InMemoryDocumentsResolver.class);
    }

    @Test
    public void singleExpressionInMemoryCandidates() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition =
          ImmutableStringCondition.of(NeFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);

      DocumentsResolver result =
          BaseResolver.resolve(expression, context, candidatesResolver, documentProperties);

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
      BaseCondition condition1 =
          ImmutableStringCondition.of(GtFilterOperation.of(), "find-me", documentProperties);
      BaseCondition condition2 =
          ImmutableStringCondition.of(LtFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath, condition1, 0);
      FilterExpression expression2 = ImmutableFilterExpression.of(filterPath, condition2, 1);

      DocumentsResolver result =
          BaseResolver.resolve(And.of(expression1, expression2), context, documentProperties);

      assertThat(result).isInstanceOf(PersistenceDocumentsResolver.class);
    }

    @Test
    public void andWithSingleExpressionDoesNotFail() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition1 =
          ImmutableStringCondition.of(GtFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath, condition1, 0);

      DocumentsResolver result =
          BaseResolver.resolve(And.of(expression1), context, documentProperties);

      assertThat(result).isInstanceOf(PersistenceDocumentsResolver.class);
    }

    @Test
    public void andWithNegation() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition1 =
          ImmutableStringCondition.of(GtFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath, condition1, 0);

      DocumentsResolver result =
          BaseResolver.resolve(Not.of(And.of(expression1)), context, documentProperties);

      assertThat(result)
          .isInstanceOfSatisfying(
              PersistenceDocumentsResolver.class,
              resolver -> {
                assertThat(resolver)
                    .extracting("queryBuilder")
                    .isInstanceOfSatisfying(
                        FilterExpressionSearchQueryBuilder.class,
                        qb ->
                            assertThat(qb)
                                .extracting("expressions")
                                .isInstanceOfSatisfying(
                                    Collection.class,
                                    expressions -> {
                                      assertThat(expressions).hasSize(1);
                                      assertThat(expressions.iterator().next())
                                          .isInstanceOfSatisfying(
                                              FilterExpression.class,
                                              filter -> {
                                                assertThat(
                                                        filter
                                                            .getCondition()
                                                            .getFilterOperationCode())
                                                    .isEqualTo(
                                                        FilterOperationCode.LTE); // inverse of "GT"
                                              });
                                    }));
              });
    }

    @Test
    public void orOnSamePath() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition1 =
          ImmutableStringCondition.of(GtFilterOperation.of(), "find-me", documentProperties);
      BaseCondition condition2 =
          ImmutableStringCondition.of(LtFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath, condition1, 0);
      FilterExpression expression2 = ImmutableFilterExpression.of(filterPath, condition2, 1);

      DocumentsResolver result =
          BaseResolver.resolve(Or.of(expression1, expression2), context, documentProperties);

      assertThat(result).isInstanceOf(OrExpressionDocumentsResolver.class);
    }

    @Test
    public void orWithSingleExpressionDoesNotFail() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition1 =
          ImmutableStringCondition.of(GtFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath, condition1, 0);

      DocumentsResolver result =
          BaseResolver.resolve(Or.of(expression1), context, documentProperties);

      assertThat(result).isInstanceOf(PersistenceDocumentsResolver.class);
    }
  }
}
