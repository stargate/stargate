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

package io.stargate.sgv2.docsapi.service.query.search.weight;

import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import java.util.Collection;
import java.util.Comparator;
import java.util.function.BiFunction;

/**
 * Interface that can resolve what {@link FilterExpression} should be executed first.
 *
 * @param <T>
 */
public interface ExpressionWeightResolver<T extends FilterExpression> {

  Comparator<BaseCondition> CONDITION_COMPARATOR =
      Comparator.comparing(BaseCondition::isPersistenceCondition)
          .thenComparing(c -> !c.isEvaluateOnMissingFields())
          .reversed();

  /**
   * Compares two expressions and resolves which one should be executed first.
   *
   * @param o1 first
   * @param o2 second
   * @return result by the comparator contract
   */
  default int compare(T o1, T o2) {
    return CONDITION_COMPARATOR.compare(o1.getCondition(), o2.getCondition());
  }

  /**
   * Compares two expression collections and resolves which one should be executed first.
   *
   * @param c1 first
   * @param c2 second
   * @return result by the comparator contract
   */
  default int compare(Collection<T> c1, Collection<T> c2) {
    int result = 0;
    for (T e1 : c1) {
      for (T e2 : c2) {
        result += compare(e1, e2);
      }
    }
    return result;
  }

  /**
   * @return Returns expression that should be executed first from two.
   */
  default BiFunction<T, T, T> single() {
    return (e1, e2) -> {
      int compare = compare(e1, e2);
      return compare > 0 ? e2 : e1;
    };
  }

  /**
   * @return Returns collection of expression that should be executed first from two.
   */
  default BiFunction<Collection<T>, Collection<T>, Collection<T>> collection() {
    return (c1, c2) -> {
      int compare = compare(c1, c2);
      return compare > 0 ? c2 : c1;
    };
  }
}
