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

package io.stargate.sgv2.docsapi.service.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Not;
import com.bpodgursky.jbool_expressions.Or;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.bridge.grpc.Values;
import io.stargate.sgv2.common.cql.builder.Literal;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.DocsApiConfiguration;
import io.stargate.sgv2.docsapi.service.query.condition.ConditionParser;
import io.stargate.sgv2.docsapi.service.query.condition.impl.BooleanCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.GenericCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.NumberCondition;
import io.stargate.sgv2.docsapi.service.query.condition.impl.StringCondition;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterOperationCode;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.GteFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.InFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.LteFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.NeFilterOperation;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

// INT test in terms that it works against components and has no mocks
// TODO ISE: move to the testing when DI allows
@ExtendWith(MockitoExtension.class)
class ExpressionParserIntTest {

  ExpressionParser service;

  ObjectMapper mapper = new ObjectMapper();

  @Mock DocsApiConfiguration configuration;

  @BeforeEach
  public void init() {
    service = new ExpressionParser(new ConditionParser(), configuration);
  }

  @Nested
  class ConstructFilterExpression {

    @Test
    public void single() throws Exception {
      String json = "{\"myField\": {\"$eq\": \"some-value\"}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              FilterExpression.class,
              c -> {
                assertThat(c.getOrderIndex()).isZero();
                assertThat(c.getSelectivity()).isEqualTo(1.0); // default selectivity
                assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                assertThat(c.getFilterPath().getParentPath()).isEmpty();
                assertThat(c.getCondition())
                    .isInstanceOfSatisfying(
                        StringCondition.class,
                        sc -> {
                          assertThat(sc.getBuiltCondition())
                              .hasValueSatisfying(
                                  builtCondition -> {
                                    assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                                    assertThat(((Literal) builtCondition.value()).get())
                                        .isEqualTo(Values.of("some-value"));
                                  });
                          assertThat(sc.getFilterOperation()).isEqualTo(EqFilterOperation.of());
                          assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
              });
    }

    @Test
    public void andWrappingRespected() throws Exception {
      String json = "{\"b\": {\"$ne\": \"some-value\"},\"a\": {\"$gt\": \"some-value\"}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              And.class,
              a -> {
                List<?> children = a.getChildren();
                assertThat(children)
                    .hasSize(2)
                    .anySatisfy(
                        c ->
                            assertThat(c)
                                .isInstanceOfSatisfying(
                                    FilterExpression.class,
                                    first -> {
                                      assertThat(first.getOrderIndex()).isZero();
                                      assertThat(first.getFilterPath().getField()).isEqualTo("b");
                                    }))
                    .anySatisfy(
                        c ->
                            assertThat(c)
                                .isInstanceOfSatisfying(
                                    FilterExpression.class,
                                    second -> {
                                      assertThat(second.getOrderIndex()).isOne();
                                      assertThat(second.getFilterPath().getField()).isEqualTo("a");
                                    }));
              });
    }

    @Test
    public void negationDirect() throws Exception {
      String json = "{\"$not\": {\"a\": {\"$eq\": \"b\"}}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result).isInstanceOf(Not.class);
      assertThat(result.getChildren()).hasSize(1);
      assertThat(result.getChildren().get(0))
          .isInstanceOfSatisfying(
              FilterExpression.class,
              filter -> {
                assertThat(filter.getOrderIndex()).isEqualTo(0);
                assertThat(filter.getCondition().getFilterOperationCode())
                    .isEqualTo(FilterOperationCode.EQ);
                assertThat(filter.getCondition().getQueryValue()).isEqualTo("b");
              });
    }

    @Test
    public void implicitAndWithNegation() throws Exception {
      String json = "{\"a\": {\"$eq\": \"b\"}, \"$not\": {\"c\": {\"$eq\": \"d\"}}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result).isInstanceOf(And.class);
      assertThat(result.getChildren()).hasSize(2);
      assertThat(result.getChildren().get(0))
          .isInstanceOfSatisfying(
              FilterExpression.class,
              filter -> {
                assertThat(filter.getOrderIndex()).isEqualTo(0);
                assertThat(filter.getCondition().getFilterOperationCode())
                    .isEqualTo(FilterOperationCode.EQ);
                assertThat(filter.getFilterPath().getPathString()).isEqualTo("a");
                assertThat(filter.getCondition().getQueryValue()).isEqualTo("b");
              });
      assertThat(result.getChildren().get(1))
          .isInstanceOfSatisfying(
              Not.class,
              negated -> {
                assertThat(negated.getChildren()).hasSize(1);
                assertThat(negated.getChildren().get(0))
                    .isInstanceOfSatisfying(
                        FilterExpression.class,
                        filter -> {
                          assertThat(filter.getOrderIndex()).isEqualTo(1);
                          assertThat(filter.getCondition().getFilterOperationCode())
                              .isEqualTo(FilterOperationCode.EQ);
                          assertThat(filter.getFilterPath().getPathString()).isEqualTo("c");
                          assertThat(filter.getCondition().getQueryValue()).isEqualTo("d");
                        });
              });
    }

    @Test
    public void negationArray() throws Exception {
      String json = "{\"$not\": [{\"a\": {\"$eq\": \"b\"}}] }";
      JsonNode root = mapper.readTree(json);

      assertThatThrownBy(
              () -> service.constructFilterExpression(Collections.emptyList(), root, false))
          .hasMessageContaining("The $not operator requires a json object as value.")
          .isInstanceOfSatisfying(
              ErrorCodeRuntimeException.class,
              e -> {
                assertThat(e.getErrorCode()).isEqualTo(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID);
              });
    }

    @Test
    public void negationMultiple() throws Exception {
      String json = "{\"$not\": {\"a\": {\"$eq\": \"b\"}, \"c\": {\"$eq\": \"d\"}}}";
      JsonNode root = mapper.readTree(json);

      assertThatThrownBy(
              () -> service.constructFilterExpression(Collections.emptyList(), root, false))
          .hasMessageContaining("The $not operator requires exactly one child expression.")
          .isInstanceOfSatisfying(
              ErrorCodeRuntimeException.class,
              e -> {
                assertThat(e.getErrorCode()).isEqualTo(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID);
              });
    }

    @Test
    public void negationEmpty() throws Exception {
      String json = "{\"$not\": {}}";
      JsonNode root = mapper.readTree(json);

      assertThatThrownBy(
              () -> service.constructFilterExpression(Collections.emptyList(), root, false))
          .hasMessageContaining("The $not operator requires exactly one child expression.")
          .isInstanceOfSatisfying(
              ErrorCodeRuntimeException.class,
              e -> {
                assertThat(e.getErrorCode()).isEqualTo(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID);
              });
    }

    @Test
    public void complex() throws Exception {
      // a & (b | (c & d))
      String json =
          "{\"a\": {\"$eq\": \"a\"}, \"$or\": [{\"b\": {\"$eq\": \"b\"}}, {\"$and\":[{\"c\": {\"$eq\": \"c\"}},{\"d\": {\"$eq\": \"d\"}}]}]}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              And.class,
              a -> {
                List<?> children = a.getChildren();
                assertThat(children)
                    .hasSize(2)
                    .anySatisfy(
                        c ->
                            assertThat(c)
                                .isInstanceOfSatisfying(
                                    FilterExpression.class,
                                    first -> {
                                      assertThat(first.getOrderIndex()).isZero();
                                      assertThat(first.getFilterPath().getField()).isEqualTo("a");
                                    }))
                    .anySatisfy(
                        c ->
                            assertThat(c)
                                .isInstanceOfSatisfying(
                                    Or.class,
                                    or -> {
                                      List<?> orChildren = or.getChildren();
                                      assertThat(orChildren)
                                          .hasSize(2)
                                          .anySatisfy(
                                              or1 ->
                                                  assertThat(or1)
                                                      .isInstanceOfSatisfying(
                                                          FilterExpression.class,
                                                          second -> {
                                                            assertThat(second.getOrderIndex())
                                                                .isOne();
                                                            assertThat(
                                                                    second
                                                                        .getFilterPath()
                                                                        .getField())
                                                                .isEqualTo("b");
                                                          }))
                                          .anySatisfy(
                                              or2 ->
                                                  assertThat(or2)
                                                      .isInstanceOfSatisfying(
                                                          And.class,
                                                          and -> {
                                                            List<?> andChildren = and.getChildren();
                                                            assertThat(andChildren)
                                                                .hasSize(2)
                                                                .anySatisfy(
                                                                    and1 ->
                                                                        assertThat(and1)
                                                                            .isInstanceOfSatisfying(
                                                                                FilterExpression
                                                                                    .class,
                                                                                third -> {
                                                                                  assertThat(
                                                                                          third
                                                                                              .getOrderIndex())
                                                                                      .isEqualTo(2);
                                                                                  assertThat(
                                                                                          third
                                                                                              .getFilterPath()
                                                                                              .getField())
                                                                                      .isEqualTo(
                                                                                          "c");
                                                                                }))
                                                                .anySatisfy(
                                                                    and2 ->
                                                                        assertThat(and2)
                                                                            .isInstanceOfSatisfying(
                                                                                FilterExpression
                                                                                    .class,
                                                                                fourth -> {
                                                                                  assertThat(
                                                                                          fourth
                                                                                              .getOrderIndex())
                                                                                      .isEqualTo(3);
                                                                                  assertThat(
                                                                                          fourth
                                                                                              .getFilterPath()
                                                                                              .getField())
                                                                                      .isEqualTo(
                                                                                          "d");
                                                                                }));
                                                          }));
                                    }));
              });
    }

    @Test
    public void singleFieldString() throws Exception {
      String json = "{\"myField\": {\"$lt\": \"some-value\"}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              FilterExpression.class,
              c -> {
                assertThat(c.getOrderIndex()).isZero();
                assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                assertThat(c.getFilterPath().getParentPath()).isEmpty();
                assertThat(c.getCondition())
                    .isInstanceOfSatisfying(
                        StringCondition.class,
                        sc -> {
                          assertThat(sc.getBuiltCondition())
                              .hasValueSatisfying(
                                  builtCondition -> {
                                    assertThat(builtCondition.predicate()).isEqualTo(Predicate.LT);
                                    assertThat(((Literal) builtCondition.value()).get())
                                        .isEqualTo(Values.of("some-value"));
                                  });
                          assertThat(sc.getFilterOperation()).isEqualTo(LtFilterOperation.of());
                          assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
              });
    }

    @Test
    public void singleFieldBoolean() throws Exception {
      String json = "{\"myField\": {\"$gte\": true}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              FilterExpression.class,
              c -> {
                assertThat(c.getOrderIndex()).isZero();
                assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                assertThat(c.getFilterPath().getParentPath()).isEmpty();
                assertThat(c.getCondition())
                    .isInstanceOfSatisfying(
                        BooleanCondition.class,
                        sc -> {
                          assertThat(sc.getBuiltCondition())
                              .hasValueSatisfying(
                                  builtCondition -> {
                                    assertThat(builtCondition.predicate()).isEqualTo(Predicate.GTE);
                                    assertThat(((Literal) builtCondition.value()).get())
                                        .isEqualTo(Values.of(Boolean.TRUE));
                                  });
                          assertThat(sc.getFilterOperation()).isEqualTo(GteFilterOperation.of());
                          assertThat(sc.getQueryValue()).isEqualTo(true);
                        });
              });
    }

    @Test
    public void singleFieldDouble() throws Exception {
      String json = "{\"myField\": {\"$lte\": 22}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              FilterExpression.class,
              c -> {
                assertThat(c.getOrderIndex()).isZero();
                assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                assertThat(c.getFilterPath().getParentPath()).isEmpty();
                assertThat(c.getCondition())
                    .isInstanceOfSatisfying(
                        NumberCondition.class,
                        sc -> {
                          assertThat(sc.getBuiltCondition())
                              .hasValueSatisfying(
                                  builtCondition -> {
                                    assertThat(builtCondition.predicate()).isEqualTo(Predicate.LTE);
                                    assertThat(((Literal) builtCondition.value()).get())
                                        .isEqualTo(Values.of(22d));
                                  });
                          assertThat(sc.getFilterOperation()).isEqualTo(LteFilterOperation.of());
                          assertThat(sc.getQueryValue()).isEqualTo(22);
                        });
              });
    }

    @Test
    public void singleFieldExtraPath() throws Exception {
      String json = "{\"my.filter.field\": {\"$eq\": \"some-value\"}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              FilterExpression.class,
              c -> {
                assertThat(c.getOrderIndex()).isZero();
                assertThat(c.getFilterPath().getField()).isEqualTo("field");
                assertThat(c.getFilterPath().getParentPath()).containsExactly("my", "filter");
                assertThat(c.getCondition())
                    .isInstanceOfSatisfying(
                        StringCondition.class,
                        sc -> {
                          assertThat(sc.getBuiltCondition())
                              .hasValueSatisfying(
                                  builtCondition -> {
                                    assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                                    assertThat(((Literal) builtCondition.value()).get())
                                        .isEqualTo(Values.of("some-value"));
                                  });
                          assertThat(sc.getFilterOperation()).isEqualTo(EqFilterOperation.of());
                          assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
              });
    }

    @Test
    public void singleFieldArrayIndex() throws Exception {
      Mockito.when(configuration.getMaxArrayLength()).thenReturn(100000);
      String json = "{\"my.filters.[2].field\": {\"$eq\": \"some-value\"}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              FilterExpression.class,
              c -> {
                assertThat(c.getOrderIndex()).isZero();
                assertThat(c.getFilterPath().getField()).isEqualTo("field");
                assertThat(c.getFilterPath().getParentPath())
                    .containsExactly("my", "filters", "[000002]");
                assertThat(c.getCondition())
                    .isInstanceOfSatisfying(
                        StringCondition.class,
                        sc -> {
                          assertThat(sc.getBuiltCondition())
                              .hasValueSatisfying(
                                  builtCondition -> {
                                    assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                                    assertThat(((Literal) builtCondition.value()).get())
                                        .isEqualTo(Values.of("some-value"));
                                  });
                          assertThat(sc.getFilterOperation()).isEqualTo(EqFilterOperation.of());
                          assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
              });
    }

    @Test
    public void singleFieldArraySplitIndex() throws Exception {
      Mockito.when(configuration.getMaxArrayLength()).thenReturn(100000);
      String json = "{\"my.filters.[1],[2].field\": {\"$eq\": \"some-value\"}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              FilterExpression.class,
              c -> {
                assertThat(c.getOrderIndex()).isZero();
                assertThat(c.getFilterPath().getField()).isEqualTo("field");
                assertThat(c.getFilterPath().getParentPath())
                    .containsExactly("my", "filters", "[000001],[000002]");
                assertThat(c.getCondition())
                    .isInstanceOfSatisfying(
                        StringCondition.class,
                        sc -> {
                          assertThat(sc.getBuiltCondition())
                              .hasValueSatisfying(
                                  builtCondition -> {
                                    assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                                    assertThat(((Literal) builtCondition.value()).get())
                                        .isEqualTo(Values.of("some-value"));
                                  });
                          assertThat(sc.getFilterOperation()).isEqualTo(EqFilterOperation.of());
                          assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
              });
    }

    @Test
    public void singleFieldPrepend() throws Exception {
      String json = "{\"my.*.field\": {\"$eq\": \"some-value\"}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Arrays.asList("first", "second"), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              FilterExpression.class,
              c -> {
                assertThat(c.getOrderIndex()).isZero();
                assertThat(c.getFilterPath().getField()).isEqualTo("field");
                assertThat(c.getFilterPath().getParentPath())
                    .containsExactly("first", "second", "my", "*");
                assertThat(c.getCondition())
                    .isInstanceOfSatisfying(
                        StringCondition.class,
                        sc -> {
                          assertThat(sc.getBuiltCondition())
                              .hasValueSatisfying(
                                  builtCondition -> {
                                    assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                                    assertThat(((Literal) builtCondition.value()).get())
                                        .isEqualTo(Values.of("some-value"));
                                  });
                          assertThat(sc.getFilterOperation()).isEqualTo(EqFilterOperation.of());
                          assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
              });
    }

    @Test
    public void singleFieldMultipleConditions() throws Exception {
      String json =
          "{\"myField\": {\"$eq\": \"some-value\", \"$in\": [\"array-one\", \"array-two\"]}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result).isInstanceOf(And.class);
      List<? extends Expression<?>> children = ((And<?>) result).getChildren();
      assertThat(children)
          .hasSize(2)
          .anySatisfy(
              e ->
                  assertThat(e)
                      .isInstanceOfSatisfying(
                          FilterExpression.class,
                          c -> {
                            assertThat(c.getOrderIndex()).isZero();
                            assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                            assertThat(c.getFilterPath().getParentPath()).isEmpty();
                            assertThat(c.getCondition())
                                .isInstanceOfSatisfying(
                                    StringCondition.class,
                                    sc -> {
                                      assertThat(sc.getBuiltCondition())
                                          .hasValueSatisfying(
                                              builtCondition -> {
                                                assertThat(builtCondition.predicate())
                                                    .isEqualTo(Predicate.EQ);
                                                assertThat(((Literal) builtCondition.value()).get())
                                                    .isEqualTo(Values.of("some-value"));
                                              });
                                      assertThat(sc.getFilterOperation())
                                          .isEqualTo(EqFilterOperation.of());
                                      assertThat(sc.getQueryValue()).isEqualTo("some-value");
                                    });
                          }))
          .anySatisfy(
              e ->
                  assertThat(e)
                      .isInstanceOfSatisfying(
                          FilterExpression.class,
                          c -> {
                            assertThat(c.getOrderIndex()).isOne();
                            assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                            assertThat(c.getFilterPath().getParentPath()).isEmpty();
                            assertThat(c.getCondition())
                                .isInstanceOfSatisfying(
                                    GenericCondition.class,
                                    sc -> {
                                      Optional<?> builtCondition = sc.getBuiltCondition();
                                      assertThat(builtCondition).isEmpty();
                                      assertThat(sc.getFilterOperation())
                                          .isEqualTo(InFilterOperation.of());
                                      assertThat(sc.getQueryValue())
                                          .isInstanceOfSatisfying(
                                              List.class,
                                              qv -> {
                                                List<?> list = qv;
                                                assertThat(list).hasSize(2);
                                                assertThat(list).element(0).isEqualTo("array-one");
                                                assertThat(list).element(1).isEqualTo("array-two");
                                              });
                                    });
                          }));
    }

    @Test
    public void multipleFields() throws Exception {
      String json =
          "{\"myField\": {\"$eq\": \"some-value\"}, \"myOtherField\": {\"$ne\": \"some-small-value\"}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);
      assertThat(result).isInstanceOf(And.class);

      List<? extends Expression<?>> children = ((And<?>) result).getChildren();
      assertThat(children)
          .hasSize(2)
          .anySatisfy(
              e ->
                  assertThat(e)
                      .isInstanceOfSatisfying(
                          FilterExpression.class,
                          c -> {
                            assertThat(c.getOrderIndex()).isZero();
                            assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                            assertThat(c.getFilterPath().getParentPath()).isEmpty();
                            assertThat(c.getCondition())
                                .isInstanceOfSatisfying(
                                    StringCondition.class,
                                    sc -> {
                                      assertThat(sc.getBuiltCondition())
                                          .hasValueSatisfying(
                                              builtCondition -> {
                                                assertThat(builtCondition.predicate())
                                                    .isEqualTo(Predicate.EQ);
                                                assertThat(((Literal) builtCondition.value()).get())
                                                    .isEqualTo(Values.of("some-value"));
                                              });
                                      assertThat(sc.getFilterOperation())
                                          .isEqualTo(EqFilterOperation.of());
                                      assertThat(sc.getQueryValue()).isEqualTo("some-value");
                                    });
                          }))
          .anySatisfy(
              e ->
                  assertThat(e)
                      .isInstanceOfSatisfying(
                          FilterExpression.class,
                          c -> {
                            assertThat(c.getOrderIndex()).isOne();
                            assertThat(c.getFilterPath().getField()).isEqualTo("myOtherField");
                            assertThat(c.getFilterPath().getParentPath()).isEmpty();
                            assertThat(c.getCondition())
                                .isInstanceOfSatisfying(
                                    StringCondition.class,
                                    sc -> {
                                      assertThat(sc.getBuiltCondition()).isEmpty();
                                      assertThat(sc.getFilterOperation())
                                          .isEqualTo(NeFilterOperation.of());
                                      assertThat(sc.getQueryValue()).isEqualTo("some-small-value");
                                    });
                          }));
    }

    @Test
    public void orCondition() throws Exception {
      String json =
          "{\"$or\": [{\"myField\": {\"$eq\": \"some-value\"}}, {\"myOtherField\": {\"$ne\": \"some-small-value\"}}]}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .satisfies(
              e ->
                  assertThat(e)
                      .isInstanceOfSatisfying(
                          Or.class,
                          or -> {
                            List<?> children = or.getChildren();
                            assertThat(children)
                                .hasSize(2)
                                .anySatisfy(
                                    c ->
                                        assertThat(c)
                                            .isInstanceOfSatisfying(
                                                FilterExpression.class,
                                                f1 -> {
                                                  assertThat(f1.getOrderIndex()).isZero();
                                                  assertThat(f1.getFilterPath().getField())
                                                      .isEqualTo("myField");
                                                  assertThat(f1.getFilterPath().getParentPath())
                                                      .isEmpty();
                                                  assertThat(f1.getCondition())
                                                      .isInstanceOfSatisfying(
                                                          StringCondition.class,
                                                          sc -> {
                                                            assertThat(sc.getBuiltCondition())
                                                                .hasValueSatisfying(
                                                                    builtCondition -> {
                                                                      assertThat(
                                                                              builtCondition
                                                                                  .predicate())
                                                                          .isEqualTo(Predicate.EQ);
                                                                      assertThat(
                                                                              ((Literal)
                                                                                      builtCondition
                                                                                          .value())
                                                                                  .get())
                                                                          .isEqualTo(
                                                                              Values.of(
                                                                                  "some-value"));
                                                                    });
                                                            assertThat(sc.getFilterOperation())
                                                                .isEqualTo(EqFilterOperation.of());
                                                            assertThat(sc.getQueryValue())
                                                                .isEqualTo("some-value");
                                                          });
                                                }))
                                .anySatisfy(
                                    c ->
                                        assertThat(c)
                                            .isInstanceOfSatisfying(
                                                FilterExpression.class,
                                                f2 -> {
                                                  assertThat(f2.getOrderIndex()).isOne();
                                                  assertThat(f2.getFilterPath().getField())
                                                      .isEqualTo("myOtherField");
                                                  assertThat(f2.getFilterPath().getParentPath())
                                                      .isEmpty();
                                                  assertThat(f2.getCondition())
                                                      .isInstanceOfSatisfying(
                                                          StringCondition.class,
                                                          sc -> {
                                                            assertThat(sc.getBuiltCondition())
                                                                .isEmpty();
                                                            assertThat(sc.getFilterOperation())
                                                                .isEqualTo(NeFilterOperation.of());
                                                            assertThat(sc.getQueryValue())
                                                                .isEqualTo("some-small-value");
                                                          });
                                                }));
                          }));
    }

    @Test
    public void orConditionNotInJsonArrayNode() throws Exception {
      String json = "{\"$or\": {\"myField\": {\"$eq\": \"b\"}}}";
      JsonNode root = mapper.readTree(json);

      Throwable t =
          catchThrowable(
              () -> service.constructFilterExpression(Collections.emptyList(), root, false));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_SEARCH_FILTER_INVALID);
    }

    @Test
    public void andCondition() throws Exception {
      String json =
          "{\"$and\": [{\"myField\": {\"$eq\": \"some-value\"}}, {\"myOtherField\": {\"$ne\": \"some-small-value\"}}]}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .satisfies(
              e ->
                  assertThat(e)
                      .isInstanceOfSatisfying(
                          And.class,
                          and -> {
                            List<?> children = and.getChildren();
                            assertThat(children)
                                .hasSize(2)
                                .anySatisfy(
                                    c ->
                                        assertThat(c)
                                            .isInstanceOfSatisfying(
                                                FilterExpression.class,
                                                f1 -> {
                                                  assertThat(f1.getOrderIndex()).isZero();
                                                  assertThat(f1.getFilterPath().getField())
                                                      .isEqualTo("myField");
                                                  assertThat(f1.getFilterPath().getParentPath())
                                                      .isEmpty();
                                                  assertThat(f1.getCondition())
                                                      .isInstanceOfSatisfying(
                                                          StringCondition.class,
                                                          sc -> {
                                                            assertThat(sc.getBuiltCondition())
                                                                .hasValueSatisfying(
                                                                    builtCondition -> {
                                                                      assertThat(
                                                                              builtCondition
                                                                                  .predicate())
                                                                          .isEqualTo(Predicate.EQ);
                                                                      assertThat(
                                                                              ((Literal)
                                                                                      builtCondition
                                                                                          .value())
                                                                                  .get())
                                                                          .isEqualTo(
                                                                              Values.of(
                                                                                  "some-value"));
                                                                    });
                                                            assertThat(sc.getFilterOperation())
                                                                .isEqualTo(EqFilterOperation.of());
                                                            assertThat(sc.getQueryValue())
                                                                .isEqualTo("some-value");
                                                          });
                                                }))
                                .anySatisfy(
                                    c ->
                                        assertThat(c)
                                            .isInstanceOfSatisfying(
                                                FilterExpression.class,
                                                f2 -> {
                                                  assertThat(f2.getOrderIndex()).isOne();
                                                  assertThat(f2.getFilterPath().getField())
                                                      .isEqualTo("myOtherField");
                                                  assertThat(f2.getFilterPath().getParentPath())
                                                      .isEmpty();
                                                  assertThat(f2.getCondition())
                                                      .isInstanceOfSatisfying(
                                                          StringCondition.class,
                                                          sc -> {
                                                            assertThat(sc.getBuiltCondition())
                                                                .isEmpty();
                                                            assertThat(sc.getFilterOperation())
                                                                .isEqualTo(NeFilterOperation.of());
                                                            assertThat(sc.getQueryValue())
                                                                .isEqualTo("some-small-value");
                                                          });
                                                }));
                          }));
    }

    @Test
    public void notObjectNode() throws Exception {
      String json = "[\"b\"]";
      JsonNode root = mapper.readTree(json);

      Throwable t =
          catchThrowable(
              () -> service.constructFilterExpression(Collections.emptyList(), root, false));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_SEARCH_OBJECT_REQUIRED);
    }

    @Test
    public void andConditionNotInJsonArrayNode() throws Exception {
      String json = "{\"$and\": {\"myField\": {\"$eq\": \"b\"}}}";
      JsonNode root = mapper.readTree(json);

      Throwable t =
          catchThrowable(
              () -> service.constructFilterExpression(Collections.emptyList(), root, false));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_SEARCH_FILTER_INVALID);
    }
  }

  @Nested
  class ConstructFilterWithSelectivity {
    @Test
    public void singleField() throws Exception {
      String json = "{\"my.*.field\": {\"$eq\": \"some-value\", \"$selectivity\": 0.123}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              FilterExpression.class,
              c -> {
                assertThat(c.getOrderIndex()).isZero();
                assertThat(c.getSelectivity()).isEqualTo(0.123);
                assertThat(c.getFilterPath().getField()).isEqualTo("field");
                assertThat(c.getCondition()).isInstanceOf(StringCondition.class);
              });
    }

    @Test
    public void multipleFields() throws Exception {
      String json =
          "{\"field1\": {\"$eq\": \"some-value\", \"$selectivity\": 0.111},"
              + "\"field2\": {\"$selectivity\": 0.222, \"$ne\": \"some-small-value\"},"
              + "\"field3\": {\"$ne\": \"some-small-value\"}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);
      assertThat(result).isInstanceOf(And.class);
      List<? extends Expression<?>> children = ((And<?>) result).getChildren();

      assertThat(children)
          .hasSize(3)
          .anySatisfy(
              e ->
                  assertThat(e)
                      .isInstanceOfSatisfying(
                          FilterExpression.class,
                          c -> {
                            assertThat(c.getFilterPath().getField()).isEqualTo("field1");
                            assertThat(c.getOrderIndex()).isEqualTo(0);
                            assertThat(c.getSelectivity()).isEqualTo(0.111);
                          }))
          .anySatisfy(
              e ->
                  assertThat(e)
                      .isInstanceOfSatisfying(
                          FilterExpression.class,
                          c -> {
                            assertThat(c.getFilterPath().getField()).isEqualTo("field2");
                            assertThat(c.getOrderIndex()).isEqualTo(1);
                            assertThat(c.getSelectivity()).isEqualTo(0.222);
                          }))
          .anySatisfy(
              e ->
                  assertThat(e)
                      .isInstanceOfSatisfying(
                          FilterExpression.class,
                          c -> {
                            assertThat(c.getFilterPath().getField()).isEqualTo("field3");
                            assertThat(c.getOrderIndex()).isEqualTo(2);
                            assertThat(c.getSelectivity()).isEqualTo(1.0); // default selectivity
                          }));
    }

    @Test
    public void singleFieldMultipleConditions() throws Exception {
      String json = "{\"my.*.field\": {\"$eq\": \"some-value\", \"$selectivity\": 0.123}}";
      JsonNode root = mapper.readTree(json);

      Expression<FilterExpression> result =
          service.constructFilterExpression(Collections.emptyList(), root, false);

      assertThat(result)
          .isInstanceOfSatisfying(
              FilterExpression.class,
              c -> {
                assertThat(c.getOrderIndex()).isZero();
                assertThat(c.getSelectivity()).isEqualTo(0.123);
                assertThat(c.getFilterPath().getField()).isEqualTo("field");
                assertThat(c.getCondition()).isInstanceOf(StringCondition.class);
              });
    }

    @Test
    public void singleSelectivityMultipleConditions() throws Exception {
      String json = "{\"field1\": {\"$gt\": \"a\", \"$lt\": \"z\", \"$selectivity\": 0.123}}";
      JsonNode root = mapper.readTree(json);

      assertThatThrownBy(
              () -> service.constructFilterExpression(Collections.emptyList(), root, false))
          .isInstanceOfSatisfying(
              ErrorCodeRuntimeException.class,
              e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID))
          .hasMessageContaining(
              "Specifying multiple filter conditions in the same JSON block with a selectivity "
                  + "hint is not supported. Combine them using \"$and\" to disambiguate. "
                  + "Related field: 'field1')");
    }

    @Test
    public void singleSelectivityNoConditions() throws Exception {
      String json = "{\"field1\": {\"$selectivity\": 0.123}}";
      JsonNode root = mapper.readTree(json);

      assertThatThrownBy(
              () -> service.constructFilterExpression(Collections.emptyList(), root, false))
          .isInstanceOfSatisfying(
              ErrorCodeRuntimeException.class,
              e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID))
          .hasMessageContaining("Field 'field1' has a selectivity hint but no condition");
    }

    @Test
    public void invalidSelectivityValueType() throws Exception {
      String json = "{\"field1\": {\"$gt\": \"a\", \"$selectivity\": \"0.123\"}}";
      JsonNode root = mapper.readTree(json);

      assertThatThrownBy(
              () -> service.constructFilterExpression(Collections.emptyList(), root, false))
          .isInstanceOfSatisfying(
              ErrorCodeRuntimeException.class,
              e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID))
          .hasMessageContaining(
              "Selectivity hint does not support the provided value \"0.123\" (expecting a number)");
    }
  }

  @Nested
  class ValueTypeCompatibility {

    @ParameterizedTest
    @CsvSource({
      "\"string-value\",String,12345,Number",
      "\"string-value\",String,true,Boolean",
      "true,Boolean,111.222,Number",
    })
    public void incompatibleValueTypes(String json1, String type1, String json2, String type2)
        throws Exception {
      String json = String.format("{\"myField\": {\"$eq\": %s, \"$gt\": %s}}", json1, json2);
      JsonNode root = mapper.readTree(json);

      assertThatThrownBy(
              () -> service.constructFilterExpression(Collections.emptyList(), root, false))
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_SEARCH_FILTER_INVALID)
          .hasMessageContaining(type1)
          .hasMessageContaining(type2);
    }

    @ParameterizedTest
    @CsvSource({"\"string-value\"", "12345", "true"})
    public void nonSpecificValueType(String jsonValue) throws Exception {
      String json =
          String.format(
              "{\"myField\": {\"$in\": [\"string\", true, 12345], \"$gt\": %s}}", jsonValue);
      JsonNode root = mapper.readTree(json);

      // Assert that no exception is thrown. Parsing is verified by other tests.
      // `$in` does not imply a specific value type, therefore it is compatible with specific
      // `$eq` conditions.
      assertThat(service.constructFilterExpression(Collections.emptyList(), root, false))
          .isNotNull();
    }
  }
}
