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

package io.stargate.web.docsapi.service.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Or;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.db.query.Predicate;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.query.condition.ConditionParser;
import io.stargate.web.docsapi.service.query.condition.impl.BooleanCondition;
import io.stargate.web.docsapi.service.query.condition.impl.GenericCondition;
import io.stargate.web.docsapi.service.query.condition.impl.NumberCondition;
import io.stargate.web.docsapi.service.query.condition.impl.StringCondition;
import io.stargate.web.docsapi.service.query.filter.operation.impl.EqFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.GteFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.InFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.LtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.LteFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.NeFilterOperation;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.core.PathSegment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

// INT test in terms that it works against components and has no mocks
// TODO ISE: move to the testing when DI allows
class ExpressionParserIntTest {

  ExpressionParser service;

  ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  public void init() {
    service = new ExpressionParser(new ConditionParser());
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
                                    assertThat(builtCondition.value().get())
                                        .isEqualTo("some-value");
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
                                    assertThat(builtCondition.value().get())
                                        .isEqualTo("some-value");
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
                                    assertThat(builtCondition.value().get())
                                        .isEqualTo(Boolean.TRUE);
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
                                    assertThat(builtCondition.value().get()).isEqualTo(22d);
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
                                    assertThat(builtCondition.value().get())
                                        .isEqualTo("some-value");
                                  });
                          assertThat(sc.getFilterOperation()).isEqualTo(EqFilterOperation.of());
                          assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
              });
    }

    @Test
    public void singleFieldArrayIndex() throws Exception {
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
                                    assertThat(builtCondition.value().get())
                                        .isEqualTo("some-value");
                                  });
                          assertThat(sc.getFilterOperation()).isEqualTo(EqFilterOperation.of());
                          assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
              });
    }

    @Test
    public void singleFieldArraySplitIndex() throws Exception {
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
                                    assertThat(builtCondition.value().get())
                                        .isEqualTo("some-value");
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

      PathSegment segment = mock(PathSegment.class);
      when(segment.getPath()).thenReturn("first").thenReturn("second");

      Expression<FilterExpression> result =
          service.constructFilterExpression(Arrays.asList(segment, segment), root, false);

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
                                    assertThat(builtCondition.value().get())
                                        .isEqualTo("some-value");
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
                                                assertThat(builtCondition.value().get())
                                                    .isEqualTo("some-value");
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
                                                assertThat(builtCondition.value().get())
                                                    .isEqualTo("some-value");
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
                                                                              builtCondition
                                                                                  .value()
                                                                                  .get())
                                                                          .isEqualTo("some-value");
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
                                                                              builtCondition
                                                                                  .value()
                                                                                  .get())
                                                                          .isEqualTo("some-value");
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
