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
import com.bpodgursky.jbool_expressions.Literal;
import com.bpodgursky.jbool_expressions.options.ExprOptions;
import com.bpodgursky.jbool_expressions.rules.Rule;
import io.stargate.web.docsapi.service.query.FilterExpression;
import java.util.function.Predicate;

/**
 * A {@link Rule} that coverts any {@link Expression<FilterExpression>} matching the given predicate
 * to {@link Literal#getTrue()}.
 */
public class TrueFilterExpressions extends Rule<Expression<FilterExpression>, FilterExpression> {

  private final Predicate<Expression<FilterExpression>> truePredicate;

  public TrueFilterExpressions(Predicate<Expression<FilterExpression>> truePredicate) {
    this.truePredicate = truePredicate;
  }

  @Override
  public Expression<FilterExpression> applyInternal(
      Expression<FilterExpression> input, ExprOptions<FilterExpression> options) {
    boolean test = truePredicate.test(input);
    if (test) {
      return Literal.getTrue();
    } else {
      return input;
    }
  }

  // only instance of FilterExpression
  @Override
  protected boolean isApply(Expression<FilterExpression> input) {
    return true;
  }
}
