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

package io.stargate.web.docsapi.service.query.search.db.impl;

import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The builder that extends the {@link FilterPathSearchQueryBuilder} and adds predicates based on
 * the collection of {@link FilterExpression}. Note that all expressions given must be related to
 * the same {@link FilterPath}.
 */
public class FilterExpressionSearchQueryBuilder extends FilterPathSearchQueryBuilder {

  private final Collection<FilterExpression> expressions;

  public FilterExpressionSearchQueryBuilder(FilterExpression expression) {
    this(Collections.singleton(expression));
  }

  public FilterExpressionSearchQueryBuilder(Collection<FilterExpression> expressions) {
    super(getFilterPath(expressions), true);
    this.expressions = expressions;
  }

  // pipe constructor to super class, no expressions defined
  protected FilterExpressionSearchQueryBuilder(FilterPath filterPath) {
    super(filterPath, true);
    this.expressions = Collections.emptyList();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Adds predicates for each expression.
   */
  @Override
  public Collection<BuiltCondition> getPredicates(int maxDepth) {
    Collection<BuiltCondition> predicates = super.getPredicates(maxDepth);

    expressions.forEach(e -> e.getCondition().getBuiltCondition().ifPresent(predicates::add));

    return predicates;
  }

  // confirms we have single filter path and extracts it
  private static FilterPath getFilterPath(Collection<FilterExpression> expressions) {
    List<FilterPath> filterPaths =
        expressions.stream()
            .map(FilterExpression::getFilterPath)
            .distinct()
            .collect(Collectors.toList());

    if (filterPaths.size() != 1) {
      throw new IllegalArgumentException(
          "FilterExpressionSearchQueryBuilder accepts only expressions with same path.");
    }

    return filterPaths.get(0);
  }
}
