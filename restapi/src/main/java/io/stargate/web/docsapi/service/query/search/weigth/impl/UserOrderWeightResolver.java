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

import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.search.weigth.ExpressionWeightResolver;
import java.util.Collection;

/** The {@link ExpressionWeightResolver} that respects the user oder of the expressions. */
public class UserOrderWeightResolver implements ExpressionWeightResolver<FilterExpression> {

  private static final UserOrderWeightResolver INSTANCE = new UserOrderWeightResolver();

  public static UserOrderWeightResolver of() {
    return INSTANCE;
  }

  /** {@inheritDoc} */
  @Override
  public int compare(FilterExpression o1, FilterExpression o2) {
    int result = ExpressionWeightResolver.super.compare(o1, o2);
    if (result != 0) {
      return result;
    }

    result = Double.compare(o1.getSelectivity(), o2.getSelectivity());
    if (result != 0) {
      return result;
    }

    return Integer.compare(o1.getOrderIndex(), o2.getOrderIndex());
  }

  /** {@inheritDoc} */
  @Override
  public int compare(Collection<FilterExpression> c1, Collection<FilterExpression> c2) {
    int result = ExpressionWeightResolver.super.compare(c1, c2);
    if (result != 0) {
      return result;
    }

    result = Double.compare(lowestSelectivity(c1), lowestSelectivity(c2));
    if (result != 0) {
      return result;
    }

    return Integer.compare(lowestIndex(c1), lowestIndex(c2));
  }

  private int lowestIndex(Collection<FilterExpression> collection) {
    return collection.stream()
        .mapToInt(FilterExpression::getOrderIndex)
        .min()
        .orElse(Integer.MAX_VALUE);
  }

  private double lowestSelectivity(Collection<FilterExpression> collection) {
    return collection.stream()
        .mapToDouble(FilterExpression::getSelectivity)
        .min()
        .orElse(Double.MAX_VALUE);
  }
}
