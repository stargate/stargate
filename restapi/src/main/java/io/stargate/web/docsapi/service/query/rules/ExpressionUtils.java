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

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import io.stargate.web.docsapi.service.query.FilterExpression;

public class ExpressionUtils {

  /**
   * Converts the given expression to CNF and eliminates boolean NOT operators but inverting the
   * negated {@link FilterExpression}s.
   *
   * <p>Note: If selectivity hints are not defined on all filters, the simplification process may
   * lose user-provided hints. This can be avoided by the user by providing hints on all filters or
   * by simplifying the expression up front.
   */
  public static Expression<FilterExpression> toSimplifiedCnf(
      Expression<FilterExpression> expression) {
    // Note: this will push NOTs down to the leaf (FilterExpression) level
    Expression<FilterExpression> cnf = RuleSet.toCNF(expression);

    // Resolve NOTs by inverting negated FilterExpressions
    Expression<FilterExpression> negationResolved = NegateFilters.resolve(cnf);

    // Re-simplify the expression after eliminating NOTs
    return RuleSet.simplify(negationResolved);
  }
}
