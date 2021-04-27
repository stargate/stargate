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

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.db.query.Predicate;
import io.stargate.web.docsapi.service.query.condition.ConditionProviderService;
import io.stargate.web.docsapi.service.query.condition.impl.AnyValueCondition;
import io.stargate.web.docsapi.service.query.condition.impl.BooleanCondition;
import io.stargate.web.docsapi.service.query.condition.impl.NumberCondition;
import io.stargate.web.docsapi.service.query.condition.impl.StringCondition;
import io.stargate.web.docsapi.service.query.predicate.impl.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.PathSegment;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// INT test in terms that it works against components and has no mocks
// TODO ISE: move to the testing when DI allows
class ExpressionServiceIntTest {

    ExpressionService service;

    ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    public void init() {
        service = new ExpressionService(new ConditionProviderService(), false);
    }

    @Nested
    class ConstructFilterExpression {

        @Test
        public void single() {
            ObjectNode root = mapper
                    .createObjectNode()
                    .set("myField", mapper.createObjectNode().put("$eq", "some-value"));

            Expression<FilterExpression> result = service.constructFilterExpression(Collections.emptyList(), root);

            assertThat(result).isInstanceOfSatisfying(FilterExpression.class, c -> {
                assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                assertThat(c.getFilterPath().getPath()).isEmpty();
                assertThat(c.getCondition()).isInstanceOfSatisfying(StringCondition.class, sc -> {
                    assertThat(sc.getBuiltCondition()).hasValueSatisfying(builtCondition -> {
                        assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                        assertThat(builtCondition.value().get()).isEqualTo("some-value");
                    });
                    assertThat(sc.getFilterPredicate()).isEqualTo(EqPredicate.of());
                    assertThat(sc.getQueryValue()).isEqualTo("some-value");
                });
            });
        }

        @Test
        public void andWrapping() {
            ObjectNode root = mapper.createObjectNode();
            root.set("myField", mapper.createObjectNode().put("$ne", "some-value"));
            root.set("myOtherField", mapper.createObjectNode().put("$gt", "some-value"));

            Expression<FilterExpression> result = service.constructFilterExpression(Collections.emptyList(), root);

            assertThat(result).isInstanceOfSatisfying(And.class, a -> assertThat(a.getChildren()).hasSize(2));
        }
    }

    @Nested
    class Parse {

        @Test
        public void singleFieldString() {
            ObjectNode root = mapper
                    .createObjectNode()
                    .set("myField", mapper.createObjectNode().put("$lt", "some-value"));

            List<Expression<FilterExpression>> result = service.parse(Collections.emptyList(), root);

            assertThat(result).singleElement()
                    .isInstanceOfSatisfying(FilterExpression.class, c -> {
                        assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                        assertThat(c.getFilterPath().getPath()).isEmpty();
                        assertThat(c.getCondition()).isInstanceOfSatisfying(StringCondition.class, sc -> {
                            assertThat(sc.getBuiltCondition()).hasValueSatisfying(builtCondition -> {
                                assertThat(builtCondition.predicate()).isEqualTo(Predicate.LT);
                                assertThat(builtCondition.value().get()).isEqualTo("some-value");
                            });
                            assertThat(sc.getFilterPredicate()).isEqualTo(LtPredicate.of());
                            assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
                    });
        }

        @Test
        public void singleFieldBoolean() {
            ObjectNode root = mapper
                    .createObjectNode()
                    .set("myField", mapper.createObjectNode().put("$gte", true));

            List<Expression<FilterExpression>> result = service.parse(Collections.emptyList(), root);

            assertThat(result).singleElement()
                    .isInstanceOfSatisfying(FilterExpression.class, c -> {
                        assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                        assertThat(c.getFilterPath().getPath()).isEmpty();
                        assertThat(c.getCondition()).isInstanceOfSatisfying(BooleanCondition.class, sc -> {
                            assertThat(sc.getBuiltCondition()).hasValueSatisfying(builtCondition -> {
                                assertThat(builtCondition.predicate()).isEqualTo(Predicate.GTE);
                                assertThat(builtCondition.value().get()).isEqualTo(Boolean.TRUE);
                            });
                            assertThat(sc.getFilterPredicate()).isEqualTo(GtePredicate.of());
                            assertThat(sc.getQueryValue()).isEqualTo(true);
                        });
                    });
        }

        @Test
        public void singleFieldDouble() {
            ObjectNode root = mapper
                    .createObjectNode()
                    .set("myField", mapper.createObjectNode().put("$lte", 22d));

            List<Expression<FilterExpression>> result = service.parse(Collections.emptyList(), root);

            assertThat(result).singleElement()
                    .isInstanceOfSatisfying(FilterExpression.class, c -> {
                        assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                        assertThat(c.getFilterPath().getPath()).isEmpty();
                        assertThat(c.getCondition()).isInstanceOfSatisfying(NumberCondition.class, sc -> {
                            assertThat(sc.getBuiltCondition()).hasValueSatisfying(builtCondition -> {
                                assertThat(builtCondition.predicate()).isEqualTo(Predicate.LTE);
                                assertThat(builtCondition.value().get()).isEqualTo(22d);
                            });
                            assertThat(sc.getFilterPredicate()).isEqualTo(LtePredicate.of());
                            assertThat(sc.getQueryValue()).isEqualTo(22d);
                        });
                    });
        }

        @Test
        public void singleFieldExtraPath() {
            ObjectNode root = mapper
                    .createObjectNode()
                    .set("my.filter.field", mapper.createObjectNode().put("$eq", "some-value"));

            List<Expression<FilterExpression>> result = service.parse(Collections.emptyList(), root);

            assertThat(result).singleElement()
                    .isInstanceOfSatisfying(FilterExpression.class, c -> {
                        assertThat(c.getFilterPath().getField()).isEqualTo("field");
                        assertThat(c.getFilterPath().getPath()).containsExactly("my", "filter");
                        assertThat(c.getCondition()).isInstanceOfSatisfying(StringCondition.class, sc -> {
                            assertThat(sc.getBuiltCondition()).hasValueSatisfying(builtCondition -> {
                                assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                                assertThat(builtCondition.value().get()).isEqualTo("some-value");
                            });
                            assertThat(sc.getFilterPredicate()).isEqualTo(EqPredicate.of());
                            assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
                    });
        }

        @Test
        public void singleFieldArrayIndex() {
            ObjectNode root = mapper
                    .createObjectNode()
                    .set("my.filters.[2].field", mapper.createObjectNode().put("$eq", "some-value"));

            List<Expression<FilterExpression>> result = service.parse(Collections.emptyList(), root);

            assertThat(result).singleElement()
                    .isInstanceOfSatisfying(FilterExpression.class, c -> {
                        assertThat(c.getFilterPath().getField()).isEqualTo("field");
                        assertThat(c.getFilterPath().getPath()).containsExactly("my", "filters", "[000002]");
                        assertThat(c.getCondition()).isInstanceOfSatisfying(StringCondition.class, sc -> {
                            assertThat(sc.getBuiltCondition()).hasValueSatisfying(builtCondition -> {
                                assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                                assertThat(builtCondition.value().get()).isEqualTo("some-value");
                            });
                            assertThat(sc.getFilterPredicate()).isEqualTo(EqPredicate.of());
                            assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
                    });
        }

        @Test
        public void singleFieldPrepend() {
            ObjectNode root = mapper
                    .createObjectNode()
                    .set("my.*.field", mapper.createObjectNode().put("$eq", "some-value"));
            PathSegment segment = mock(PathSegment.class);
            when(segment.getPath()).thenReturn("first").thenReturn("second");

            List<Expression<FilterExpression>> result = service.parse(Arrays.asList(segment, segment), root);

            assertThat(result).singleElement()
                    .isInstanceOfSatisfying(FilterExpression.class, c -> {
                        assertThat(c.getFilterPath().getField()).isEqualTo("field");
                        assertThat(c.getFilterPath().getPath()).containsExactly("first", "second", "my", "*");
                        assertThat(c.getCondition()).isInstanceOfSatisfying(StringCondition.class, sc -> {
                            assertThat(sc.getBuiltCondition()).hasValueSatisfying(builtCondition -> {
                                assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                                assertThat(builtCondition.value().get()).isEqualTo("some-value");
                            });
                            assertThat(sc.getFilterPredicate()).isEqualTo(EqPredicate.of());
                            assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
                    });
        }


        @Test
        public void singleFieldMultipleConditions() {
            ObjectNode root = mapper.createObjectNode();
            root.set("myField", mapper.createObjectNode()
                    .put("$eq", "some-value")
                    .set("$in", mapper.createArrayNode()
                            .add("array-one")
                            .add("array-two")
                    )
            );

            List<Expression<FilterExpression>> result = service.parse(Collections.emptyList(), root);

            assertThat(result).hasSize(2)
                    .anySatisfy(e -> assertThat(e).isInstanceOfSatisfying(FilterExpression.class, c -> {
                        assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                        assertThat(c.getFilterPath().getPath()).isEmpty();
                        assertThat(c.getCondition()).isInstanceOfSatisfying(StringCondition.class, sc -> {
                            assertThat(sc.getBuiltCondition()).hasValueSatisfying(builtCondition -> {
                                assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                                assertThat(builtCondition.value().get()).isEqualTo("some-value");
                            });
                            assertThat(sc.getFilterPredicate()).isEqualTo(EqPredicate.of());
                            assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
                    }))
                    .anySatisfy(e -> assertThat(e).isInstanceOfSatisfying(FilterExpression.class, c -> {
                        assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                        assertThat(c.getFilterPath().getPath()).isEmpty();
                        assertThat(c.getCondition()).isInstanceOfSatisfying(AnyValueCondition.class, sc -> {
                            Optional<?> builtCondition = sc.getBuiltCondition();
                            assertThat(builtCondition).isEmpty();
                            assertThat(sc.getFilterPredicate()).isEqualTo(InPredicate.of());
                            assertThat(sc.getQueryValue()).isInstanceOfSatisfying(List.class, qv -> {
                                List<?> list = qv;
                                assertThat(list).hasSize(2);
                                assertThat(list).element(0).isEqualTo("array-one");
                                assertThat(list).element(1).isEqualTo("array-two");
                            });
                        });
                    }));
        }

        @Test
        public void multipleFields() {
            ObjectNode root = mapper.createObjectNode();
            root.set("myField", mapper.createObjectNode().put("$eq", "some-value"));
            root.set("myOtherField", mapper.createObjectNode().put("$ne", "some-small-value"));

            List<Expression<FilterExpression>> result = service.parse(Collections.emptyList(), root);

            assertThat(result).hasSize(2)
                    .anySatisfy(e -> assertThat(e).isInstanceOfSatisfying(FilterExpression.class, c -> {
                        assertThat(c.getFilterPath().getField()).isEqualTo("myField");
                        assertThat(c.getFilterPath().getPath()).isEmpty();
                        assertThat(c.getCondition()).isInstanceOfSatisfying(StringCondition.class, sc -> {
                            assertThat(sc.getBuiltCondition()).hasValueSatisfying(builtCondition -> {
                                assertThat(builtCondition.predicate()).isEqualTo(Predicate.EQ);
                                assertThat(builtCondition.value().get()).isEqualTo("some-value");
                            });
                            assertThat(sc.getFilterPredicate()).isEqualTo(EqPredicate.of());
                            assertThat(sc.getQueryValue()).isEqualTo("some-value");
                        });
                    }))
                    .anySatisfy(e -> assertThat(e).isInstanceOfSatisfying(FilterExpression.class, c -> {
                        assertThat(c.getFilterPath().getField()).isEqualTo("myOtherField");
                        assertThat(c.getFilterPath().getPath()).isEmpty();
                        assertThat(c.getCondition()).isInstanceOfSatisfying(StringCondition.class, sc -> {
                            assertThat(sc.getBuiltCondition()).isEmpty();
                            assertThat(sc.getFilterPredicate()).isEqualTo(NePredicate.of());
                            assertThat(sc.getQueryValue()).isEqualTo("some-small-value");
                        });
                    }));
        }

    }
}
