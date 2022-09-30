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
import com.bpodgursky.jbool_expressions.Not;
import com.bpodgursky.jbool_expressions.options.ExprOptions;
import com.bpodgursky.jbool_expressions.rules.Rule;
import com.bpodgursky.jbool_expressions.rules.RuleList;
import com.bpodgursky.jbool_expressions.rules.RulesHelper;
import io.stargate.web.docsapi.service.query.FilterExpression;
import java.util.Collections;
import java.util.List;

public class NegateFilters extends Rule<Expression<FilterExpression>, FilterExpression> {

  private static final RuleList<FilterExpression> rules =
      new RuleList<>(Collections.singletonList(new NegateFilters()));

  @Override
  public Expression<FilterExpression> applyInternal(
      Expression<FilterExpression> input, ExprOptions<FilterExpression> options) {
    Not<FilterExpression> negated = (Not<FilterExpression>) input;
    FilterExpression child = (FilterExpression) negated.getChildren().get(0);

    return child.negate();
  }

  @Override
  protected boolean isApply(Expression<FilterExpression> input) {
    if (!(input instanceof Not)) {
      return false;
    }

    Not<FilterExpression> negated = (Not<FilterExpression>) input;

    List<Expression<FilterExpression>> children = negated.getChildren();
    if (children.size() != 1) {
      return false;
    }

    Expression<FilterExpression> child = children.get(0);
    return child instanceof FilterExpression;
  }

  public static Expression<FilterExpression> resolve(Expression<FilterExpression> expression) {
    return RulesHelper.applyAll(expression, rules, ExprOptions.noCaching());
  }
}
