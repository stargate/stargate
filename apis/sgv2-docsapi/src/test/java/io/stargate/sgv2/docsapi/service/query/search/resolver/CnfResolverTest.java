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
import com.bpodgursky.jbool_expressions.Or;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.FilterPath;
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterExpression;
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterPath;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableGenericCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.ImmutableStringCondition;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.GtFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.InFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.sgv2.docsapi.service.query.search.resolver.filter.impl.InMemoryCandidatesFilter;
import io.stargate.sgv2.docsapi.service.query.search.resolver.filter.impl.PersistenceCandidatesFilter;
import io.stargate.sgv2.docsapi.service.query.search.resolver.impl.AllFiltersResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.impl.AnyFiltersResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.impl.InMemoryDocumentsResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.impl.OrExpressionDocumentsResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.impl.PersistenceDocumentsResolver;
import java.util.Collections;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

@QuarkusTest
class CnfResolverTest {

  @Inject DocumentProperties documentProperties;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
  }

  @Nested
  class Resolve {

    @Test
    public void twoExpressionSamePath() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      BaseCondition condition1 =
          ImmutableStringCondition.of(GtFilterOperation.of(), "find-me", documentProperties);
      BaseCondition condition2 =
          ImmutableStringCondition.of(LtFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath, condition1, 0);
      FilterExpression expression2 = ImmutableFilterExpression.of(filterPath, condition2, 1);

      DocumentsResolver result =
          CnfResolver.resolve(And.of(expression1, expression2), context, documentProperties);

      assertThat(result).isInstanceOf(PersistenceDocumentsResolver.class);
    }

    @Test
    public void twoPersistenceExpressionDifferentPath() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath1 = ImmutableFilterPath.of(Collections.singletonList("b"));
      FilterPath filterPath2 = ImmutableFilterPath.of(Collections.singletonList("a"));
      BaseCondition condition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "find-me", documentProperties);
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath1, condition, 0);
      FilterExpression expression2 = ImmutableFilterExpression.of(filterPath2, condition, 1);

      And<FilterExpression> and = And.of(expression1, expression2);
      DocumentsResolver result = CnfResolver.resolve(and, context, documentProperties);

      // ensure not reordering
      assertThat(result)
          .isInstanceOfSatisfying(
              AllFiltersResolver.class,
              allOf -> {
                assertThat(allOf)
                    .extracting("candidatesResolver")
                    .isInstanceOfSatisfying(
                        PersistenceDocumentsResolver.class,
                        r -> {
                          assertThat(r)
                              .extracting("queryBuilder")
                              .extracting("filterPath")
                              .isEqualTo(filterPath1);
                        });
                assertThat(allOf)
                    .extracting("candidatesFilters")
                    .asList()
                    .singleElement()
                    .isInstanceOfSatisfying(
                        PersistenceCandidatesFilter.class,
                        f -> {
                          assertThat(f)
                              .extracting("queryBuilder")
                              .extracting("filterPath")
                              .isEqualTo(filterPath2);
                        });
              });
    }

    @Test
    public void twoMemoryExpressionDifferentPath() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath1 = ImmutableFilterPath.of(Collections.singletonList("b"));
      FilterPath filterPath2 = ImmutableFilterPath.of(Collections.singletonList("a"));
      BaseCondition condition =
          ImmutableGenericCondition.of(
              InFilterOperation.of(),
              Collections.singletonList("find-me"),
              documentProperties,
              false);
      FilterExpression expression1 = ImmutableFilterExpression.of(filterPath1, condition, 0);
      FilterExpression expression2 = ImmutableFilterExpression.of(filterPath2, condition, 1);

      And<FilterExpression> and = And.of(expression1, expression2);
      DocumentsResolver result = CnfResolver.resolve(and, context, documentProperties);

      // ensure not reordering
      assertThat(result)
          .isInstanceOfSatisfying(
              AllFiltersResolver.class,
              allOf -> {
                assertThat(allOf)
                    .extracting("candidatesResolver")
                    .isInstanceOfSatisfying(
                        InMemoryDocumentsResolver.class,
                        r -> {
                          assertThat(r)
                              .extracting("queryBuilder")
                              .extracting("filterPath")
                              .isEqualTo(filterPath1);
                        });
                assertThat(allOf)
                    .extracting("candidatesFilters")
                    .asList()
                    .singleElement()
                    .isInstanceOfSatisfying(
                        InMemoryCandidatesFilter.class,
                        f -> {
                          assertThat(f)
                              .extracting("queryBuilder")
                              .extracting("filterPath")
                              .isEqualTo(filterPath2);
                        });
              });
    }

    @Test
    public void mixed4Expressions() {
      ExecutionContext context = ExecutionContext.create(true);
      FilterPath filterPath1 = ImmutableFilterPath.of(Collections.singletonList("field1"));
      FilterPath filterPath2 = ImmutableFilterPath.of(Collections.singletonList("field2"));
      BaseCondition persistenceCondition =
          ImmutableStringCondition.of(GtFilterOperation.of(), "find-me", documentProperties);
      BaseCondition memoryCondition =
          ImmutableGenericCondition.of(
              InFilterOperation.of(),
              Collections.singletonList("find-me"),
              documentProperties,
              false);
      FilterExpression expression1 =
          ImmutableFilterExpression.of(filterPath1, persistenceCondition, 0);
      FilterExpression expression2 =
          ImmutableFilterExpression.of(filterPath2, persistenceCondition, 1);
      FilterExpression expression3 = ImmutableFilterExpression.of(filterPath1, memoryCondition, 2);
      FilterExpression expression4 = ImmutableFilterExpression.of(filterPath2, memoryCondition, 3);

      DocumentsResolver result =
          CnfResolver.resolve(
              And.of(expression1, expression2, expression3, expression4),
              context,
              documentProperties);

      // |
      // | -> persistence candidates (1 exp)
      // | | -> persistence filters (1 exp)
      // | -> in-memory filters (2 exp)O

      assertThat(result)
          .isInstanceOfSatisfying(
              AllFiltersResolver.class,
              allOf -> {
                assertThat(allOf)
                    .extracting("candidatesResolver")
                    .isInstanceOfSatisfying(
                        AllFiltersResolver.class,
                        allOf2 -> {
                          assertThat(allOf2)
                              .extracting("candidatesResolver")
                              .isInstanceOf(PersistenceDocumentsResolver.class);
                          assertThat(allOf2)
                              .extracting("candidatesFilters")
                              .asList()
                              .singleElement()
                              .isInstanceOf(PersistenceCandidatesFilter.class);
                        });
                assertThat(allOf)
                    .extracting("candidatesFilters")
                    .asList()
                    .hasSize(2)
                    .allSatisfy(cf -> assertThat(cf).isInstanceOf(InMemoryCandidatesFilter.class));
              });
    }
  }

  @Test
  public void mixed4ExpressionsWithOrs() {
    ExecutionContext context = ExecutionContext.create(true);
    FilterPath filterPath1 = ImmutableFilterPath.of(Collections.singletonList("field1"));
    FilterPath filterPath2 = ImmutableFilterPath.of(Collections.singletonList("field2"));
    BaseCondition prsCondition =
        ImmutableStringCondition.of(GtFilterOperation.of(), "find-me", documentProperties);
    BaseCondition memCondition =
        ImmutableGenericCondition.of(
            InFilterOperation.of(),
            Collections.singletonList("find-me"),
            documentProperties,
            false);
    FilterExpression expression1 = ImmutableFilterExpression.of(filterPath1, prsCondition, 0);
    FilterExpression expression2 = ImmutableFilterExpression.of(filterPath2, prsCondition, 1);
    FilterExpression expression3 = ImmutableFilterExpression.of(filterPath1, memCondition, 2);
    FilterExpression expression4 = ImmutableFilterExpression.of(filterPath2, memCondition, 3);

    DocumentsResolver result =
        CnfResolver.resolve(
            And.of(expression1, Or.of(expression2, expression3), expression4),
            context,
            documentProperties);

    // |
    // | -> persistence candidates (1 exp)
    // | -> or filters (2 exp)
    // | -> in-memory filters (1 exp)

    assertThat(result)
        .isInstanceOfSatisfying(
            AllFiltersResolver.class,
            allOf -> {
              assertThat(allOf)
                  .extracting("candidatesResolver")
                  .isInstanceOfSatisfying(
                      AnyFiltersResolver.class,
                      anyOf2 -> {
                        assertThat(anyOf2)
                            .extracting("candidatesResolver")
                            .isInstanceOf(PersistenceDocumentsResolver.class);
                        assertThat(anyOf2)
                            .extracting("candidatesFilters")
                            .asList()
                            .hasSize(2)
                            .anySatisfy(
                                cf -> assertThat(cf).isInstanceOf(InMemoryCandidatesFilter.class))
                            .anySatisfy(
                                cf ->
                                    assertThat(cf).isInstanceOf(PersistenceCandidatesFilter.class));
                      });
              assertThat(allOf)
                  .extracting("candidatesFilters")
                  .asList()
                  .singleElement()
                  .satisfies(cf -> assertThat(cf).isInstanceOf(InMemoryCandidatesFilter.class));
            });
  }

  @Test
  public void onlyInMemoryWithOrs() {
    ExecutionContext context = ExecutionContext.create(true);
    FilterPath filterPath1 = ImmutableFilterPath.of(Collections.singletonList("field1"));
    FilterPath filterPath2 = ImmutableFilterPath.of(Collections.singletonList("field2"));
    BaseCondition memCondition =
        ImmutableGenericCondition.of(
            InFilterOperation.of(),
            Collections.singletonList("find-me"),
            documentProperties,
            false);
    BaseCondition memCondition2 =
        ImmutableGenericCondition.of(
            InFilterOperation.of(),
            Collections.singletonList("find-me-again"),
            documentProperties,
            false);
    FilterExpression expression1 = ImmutableFilterExpression.of(filterPath1, memCondition, 0);
    FilterExpression expression2 = ImmutableFilterExpression.of(filterPath2, memCondition, 1);
    FilterExpression expression3 = ImmutableFilterExpression.of(filterPath1, memCondition2, 2);
    FilterExpression expression4 = ImmutableFilterExpression.of(filterPath2, memCondition2, 3);

    DocumentsResolver result =
        CnfResolver.resolve(
            And.of(expression1, Or.of(expression2, expression3), expression4),
            context,
            documentProperties);

    // |
    // | -> or filters (2 exp)
    // | -> in-memory filters (2 exp)

    assertThat(result)
        .isInstanceOfSatisfying(
            AllFiltersResolver.class,
            allOf -> {
              assertThat(allOf)
                  .extracting("candidatesResolver")
                  .isInstanceOfSatisfying(
                      OrExpressionDocumentsResolver.class,
                      orResolver -> {
                        assertThat(orResolver).extracting("queryBuilders").asList().hasSize(2);
                      });
              assertThat(allOf)
                  .extracting("candidatesFilters")
                  .asList()
                  .hasSize(2)
                  .allSatisfy(cf -> assertThat(cf).isInstanceOf(InMemoryCandidatesFilter.class));
            });
  }
}
