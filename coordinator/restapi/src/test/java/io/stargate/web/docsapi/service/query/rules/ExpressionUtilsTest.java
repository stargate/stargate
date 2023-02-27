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

package io.stargate.web.docsapi.service.query.rules;

import static io.stargate.web.docsapi.service.query.filter.operation.FilterOperationCode.GT;
import static io.stargate.web.docsapi.service.query.filter.operation.FilterOperationCode.LTE;
import static io.stargate.web.docsapi.service.query.rules.ExpressionUtils.toSimplifiedCnf;
import static org.assertj.core.api.Assertions.assertThat;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Not;
import com.bpodgursky.jbool_expressions.Or;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.ImmutableFilterExpression;
import io.stargate.web.docsapi.service.query.ImmutableFilterPath;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.impl.ImmutableNumberCondition;
import io.stargate.web.docsapi.service.query.filter.operation.FilterOperationCode;
import io.stargate.web.docsapi.service.query.filter.operation.ValueFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.GtFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.LteFilterOperation;
import java.util.Collections;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExpressionUtilsTest {

  private FilterExpression e(FilterOperationCode op, int value) {
    FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));

    ValueFilterOperation operation;
    switch (op) {
      case GT:
        operation = GtFilterOperation.of();
        break;
      case LTE:
        operation = LteFilterOperation.of();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported op code: " + op);
    }

    BaseCondition condition = ImmutableNumberCondition.of(operation, value);
    return ImmutableFilterExpression.of(filterPath, condition, 0);
  }

  @Nested
  class CNF {

    @Test
    void oneCondition() {
      Expression<FilterExpression> cnf = toSimplifiedCnf(e(GT, 1));
      assertThat(cnf.toLexicographicString()).isEqualTo("field GT 1");
    }

    @Test
    void simpleAnd() {
      Expression<FilterExpression> cnf = toSimplifiedCnf(And.of(e(GT, 1), e(LTE, 2)));
      assertThat(cnf.toLexicographicString()).isEqualTo("(field GT 1 & field LTE 2)");
    }

    @Test
    void simpleOr() {
      Expression<FilterExpression> cnf = toSimplifiedCnf(Or.of(e(GT, 1), e(LTE, 2)));
      assertThat(cnf.toLexicographicString()).isEqualTo("(field GT 1 | field LTE 2)");
    }

    @Test
    void orOfAnd() {
      Expression<FilterExpression> cnf =
          toSimplifiedCnf(Or.of(e(LTE, 1), And.of(e(GT, 2), e(LTE, 3))));
      assertThat(cnf.toLexicographicString())
          .isEqualTo("((field GT 2 | field LTE 1) & (field LTE 1 | field LTE 3))");
    }

    @Test
    void andOfOr() {
      Expression<FilterExpression> cnf =
          toSimplifiedCnf(And.of(e(LTE, 1), Or.of(e(GT, 2), e(LTE, 3))));
      assertThat(cnf.toLexicographicString())
          .isEqualTo("((field GT 2 | field LTE 3) & field LTE 1)");
    }

    @Test
    void wrappingOr() {
      Expression<FilterExpression> cnf =
          toSimplifiedCnf(Or.of(And.of(e(LTE, 1), Or.of(e(GT, 2), e(LTE, 3)))));
      assertThat(cnf.toLexicographicString())
          .isEqualTo("((field GT 2 | field LTE 3) & field LTE 1)");
    }

    @Test
    void orOfAndWithNegation() {
      Expression<FilterExpression> cnf =
          toSimplifiedCnf(Or.of(Not.of(e(LTE, 1)), e(GT, 2), And.of(e(GT, 3), e(LTE, 4))));
      // Note: the first predicate got inverted
      // Note: predicate-specific simplification is not supported (yet?), as in:
      // field GT 2 | field GT 3 => field GT 2
      assertThat(cnf.toLexicographicString())
          .isEqualTo(
              "((field GT 1 | field GT 2 | field GT 3) & (field GT 1 | field GT 2 | field LTE 4))");
    }
  }

  @Nested
  class Negation {
    @Test
    void negatedAnd() {
      Expression<FilterExpression> cnf = toSimplifiedCnf(Not.of(And.of(e(GT, 1), e(LTE, 2))));
      // Note: predicates got inverted
      assertThat(cnf.toLexicographicString()).isEqualTo("(field GT 2 | field LTE 1)");
    }

    @Test
    void negatedOr() {
      Expression<FilterExpression> cnf = toSimplifiedCnf(Not.of(Or.of(e(GT, 1), e(LTE, 2))));
      // Note: predicates got inverted
      assertThat(cnf.toLexicographicString()).isEqualTo("(field GT 2 & field LTE 1)");
    }
  }

  @Nested
  class Simplification {
    @Test
    void andWithSame() {
      Expression<FilterExpression> cnf = toSimplifiedCnf(And.of(e(GT, 1), e(GT, 1)));
      // Note: duplicate condition got removed
      assertThat(cnf.toLexicographicString()).isEqualTo("field GT 1");
    }

    @Test
    void andWithSameNegated() {
      Expression<FilterExpression> cnf = toSimplifiedCnf(And.of(e(GT, 1), Not.of(e(GT, 1))));
      assertThat(cnf.toLexicographicString()).isEqualTo("false");
    }

    @Test
    void orWithSameNegated() {
      Expression<FilterExpression> cnf = toSimplifiedCnf(Or.of(e(GT, 1), Not.of(e(GT, 1))));
      assertThat(cnf.toLexicographicString()).isEqualTo("true");
    }

    @Test
    void impliedAndSubExpression() {
      // Note: the first OR condition implies the AND sub-expression
      Expression<FilterExpression> cnf =
          toSimplifiedCnf(Or.of(e(GT, 2), And.of(e(GT, 2), e(LTE, 3))));
      assertThat(cnf.toLexicographicString()).isEqualTo("field GT 2");
    }

    @Test
    void impliedNegatedAndSubExpression() {
      // Note: the first OR condition implies the AND sub-expression
      Expression<FilterExpression> cnf =
          toSimplifiedCnf(Or.of(e(LTE, 2), And.of(Not.of(e(GT, 2)), e(LTE, 3))));
      assertThat(cnf.toLexicographicString()).isEqualTo("field LTE 2");
    }

    @Test
    void negatedImpliesAndSubExpression() {
      // Note: the first OR condition implies the AND sub-expression
      Expression<FilterExpression> cnf =
          toSimplifiedCnf(Or.of(Not.of(e(LTE, 2)), And.of(e(GT, 2), e(LTE, 3))));
      assertThat(cnf.toLexicographicString()).isEqualTo("field GT 2");
    }

    @Test
    void andWithEquivalentInvertedNegatedPredicate() {
      Expression<FilterExpression> cnf = toSimplifiedCnf(And.of(e(GT, 1), Not.of(e(LTE, 1))));
      assertThat(cnf.toLexicographicString()).isEqualTo("field GT 1");
    }

    @Test
    void impliedAndSubExpressionWithNegatedOrOfSame() {
      // Note: the second OR condition implies the AND sub-expression
      // Note: the first OR condition is equivalent to the second OR condition
      Expression<FilterExpression> cnf =
          toSimplifiedCnf(Or.of(Not.of(e(LTE, 2)), e(GT, 2), And.of(e(GT, 2), e(LTE, 3))));
      assertThat(cnf.toLexicographicString()).isEqualTo("field GT 2");
    }
  }
}
