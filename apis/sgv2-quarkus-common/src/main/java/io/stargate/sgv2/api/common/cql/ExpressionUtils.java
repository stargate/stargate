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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.api.common.cql;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Or;
import java.util.List;

/**
 * Convenience expression builder
 *
 * <p>when construct jbool expression without specifying a comparator, it will use hashComparator by
 * default which will cause the order of expression indeterminate, and cause JSONAPI unit tests
 * failure By using this ExpressionUtils class, we pass a default comparator to keep expression
 * order as it is
 */
public class ExpressionUtils<K> {

  public static <K> And<K> andOf(Expression<K>... expressions) {
    // expression as creation order
    return And.of(expressions, (e1, e2) -> 1);
  }

  public static <K> And<K> andOf(List<? extends Expression<K>> expressions) {
    // expression as creation order
    return And.of(expressions.toArray(new Expression[expressions.size()]), (e1, e2) -> 1);
  }

  public static <K> Or<K> orOf(List<? extends Expression<K>> expressions) {
    // expression as creation order
    return Or.of(expressions.toArray(new Expression[expressions.size()]), (e1, e2) -> 1);
  }

  public static <K> Or<K> orOf(Expression<K>... expressions) {
    // expression as creation order
    return Or.of(expressions, (e1, e2) -> 1);
  }

  public static <K> Expression<K> buildExpression(
      List<? extends Expression<K>> expressions, String logicOperator) {
    switch (logicOperator) {
      case "$and" -> {
        return andOf(expressions);
      }
      case "$or" -> {
        return orOf(expressions);
      }
      default -> {
        return null;
      }
    }
  }

  public static <K> Expression<K>[] getAsArray(Expression<K>... expressions) {
    return expressions;
  }
}
