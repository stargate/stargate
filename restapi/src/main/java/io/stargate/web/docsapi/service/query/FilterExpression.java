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

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.options.ExprOptions;
import com.bpodgursky.jbool_expressions.rules.RuleList;
import com.bpodgursky.jbool_expressions.util.ExprFactory;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import java.util.*;
import java.util.function.Function;
import org.immutables.value.Value;

/** Filter expression that stands as the node in the Expression tree. */
@Value.Immutable
public abstract class FilterExpression extends Expression<FilterExpression> {

  /** Type of the expression. */
  public static final String EXPR_TYPE = "filter";

  /** @return {@link FilterPath} for this expression */
  @Value.Parameter
  public abstract FilterPath getFilterPath();

  /** @return {@link BaseCondition} for this expression */
  @Value.Parameter
  public abstract BaseCondition getCondition();

  // below is Expression relevant implementation that targets this

  @Override
  public Expression<FilterExpression> apply(
      RuleList<FilterExpression> rules, ExprOptions<FilterExpression> cache) {
    return this;
  }

  @Override
  public List<Expression<FilterExpression>> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public Expression<FilterExpression> map(
      Function<Expression<FilterExpression>, Expression<FilterExpression>> function,
      ExprFactory<FilterExpression> factory) {
    return function.apply(this);
  }

  @Override
  public String getExprType() {
    return EXPR_TYPE;
  }

  @Override
  public Expression<FilterExpression> sort(Comparator<Expression> comparator) {
    return this;
  }

  @Override
  public void collectK(Set<FilterExpression> set, int limit) {
    if (set.size() >= limit) {
      return;
    }

    set.add(this);
  }

  @Override
  public Expression<FilterExpression> replaceVars(
      Map<FilterExpression, Expression<FilterExpression>> m,
      ExprFactory<FilterExpression> exprFactory) {
    if (m.containsKey(this)) {
      return m.get(this);
    }
    return this;
  }
}
