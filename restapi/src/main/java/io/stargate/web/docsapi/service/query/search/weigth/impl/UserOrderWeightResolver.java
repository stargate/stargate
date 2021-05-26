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

package io.stargate.web.docsapi.service.query.search.weigth.impl;

import com.bpodgursky.jbool_expressions.Expression;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.search.weigth.ExpressionWeightResolver;
import java.util.Collection;
import java.util.List;

/** The {@link ExpressionWeightResolver} that respects the user oder of the expressions. */
public class UserOrderWeightResolver implements ExpressionWeightResolver<FilterExpression> {

  private final List<Expression<FilterExpression>> order;

  public UserOrderWeightResolver(List<Expression<FilterExpression>> order) {
    this.order = order;
  }

  /** {@inheritDoc} */
  @Override
  public int compare(FilterExpression o1, FilterExpression o2) {
    int defaultCompare = ExpressionWeightResolver.super.compare(o1, o2);
    if (defaultCompare != 0) {
      return defaultCompare;
    } else {
      return Integer.compare(indexOrMax(order, o1), indexOrMax(order, o2));
    }
  }

  /** {@inheritDoc} */
  @Override
  public int compare(Collection<FilterExpression> c1, Collection<FilterExpression> c2) {
    int defaultCompare = ExpressionWeightResolver.super.compare(c1, c2);
    if (defaultCompare != 0) {
      return defaultCompare;
    } else {
      return Integer.compare(lowestIndexOrMax(order, c1), lowestIndexOrMax(order, c2));
    }
  }

  private int indexOrMax(List<Expression<FilterExpression>> order, Expression<FilterExpression> e) {
    int index = order.indexOf(e);
    if (index < 0) {
      return Integer.MAX_VALUE;
    } else {
      return index;
    }
  }

  private int lowestIndexOrMax(
      List<Expression<FilterExpression>> order, Collection<FilterExpression> collection) {
    int index = Integer.MAX_VALUE;
    for (FilterExpression e : collection) {
      int current = indexOrMax(order, e);
      if (current == 0) {
        return 0;
      } else if (current > 0 && current < index) {
        index = current;
      }
    }
    return index;
  }
}
