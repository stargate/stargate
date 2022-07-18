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

package io.stargate.sgv2.docsapi.service.query;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.options.ExprOptions;
import com.bpodgursky.jbool_expressions.rules.RuleList;
import com.bpodgursky.jbool_expressions.util.ExprFactory;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterHintCode;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.util.DocsApiUtils;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.immutables.value.Value;

/** Filter expression that stands as the node in the Expression tree. */
@Value.Immutable
public abstract class FilterExpression extends Expression<FilterExpression>
    implements Predicate<RawDocument> {

  /** Type of the expression. */
  public static final String EXPR_TYPE = "filter";

  /** @return {@link FilterPath} for this expression */
  @Value.Parameter
  public abstract FilterPath getFilterPath();

  /** @return {@link BaseCondition} for this expression */
  @Value.Parameter
  public abstract BaseCondition getCondition();

  /** @return Returns the order index of this filter expression given by the user. */
  @Value.Parameter
  public abstract int getOrderIndex();

  /**
   * The best known selectivity of this filter expression.
   *
   * <p>Selectivity is a value between 0 and 1 (inclusive) that represents the percentage of rows
   * that are expected to be matched by this filter.
   *
   * <p>By default, filters that do not have an explicit selectivity {@link
   * FilterHintCode#SELECTIVITY hint} provided by the query get the selectivity value of {@code 1.0}
   * (the worst possible selectivity).
   */
  @Value.Default
  // Do not use selectivity for equality comparisons. This is critical to allow expression
  // simplification (see `ExpressionUtilsTest`) in cases where filters do not have proper
  // selectivity hints defined (hints are optional).
  @Value.Auxiliary
  public double getSelectivity() {
    return 1.0;
  }

  public static FilterExpression of(
      FilterPath filterPath, BaseCondition condition, int orderIndex, double selectivity) {
    return ImmutableFilterExpression.builder()
        .filterPath(filterPath)
        .condition(condition)
        .orderIndex(orderIndex)
        .selectivity(selectivity)
        .build();
  }

  public FilterExpression negate() {
    return ImmutableFilterExpression.builder()
        .filterPath(getFilterPath())
        .condition(getCondition().negate())
        .orderIndex(getOrderIndex())
        // The negated filter will select rows complementing the ones selected by source filter
        .selectivity(1.0 - getSelectivity())
        .build();
  }

  /** @return Returns human-readable description of this expression. */
  public String getDescription() {
    BaseCondition condition = getCondition();
    return getFilterPath().getPathString()
        + " "
        + condition.getFilterOperationCode()
        + " "
        + condition.getQueryValue();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Tests all rows in this document, making sure that at least is one the filter path and
   * fulfills the condition. If no row is on the filter path, returns true.
   */
  @Override
  public boolean test(RawDocument rawDocument) {
    return test(rawDocument.rows());
  }

  /**
   * Tests all rows from a single document, making sure that at least is one the filter path and
   * fulfills the condition. If no row is on the filter path, returns true.
   */
  public boolean test(List<RowWrapper> documentRows) {
    boolean anyFailing = false;
    for (RowWrapper row : documentRows) {

      // if row is not matching the filter path do nothing
      if (!matchesFilterPath(row)) {
        continue;
      }

      // do a test
      boolean test = getCondition().test(row);

      // if the condition is passed, whole document passes
      // otherwise, continue in hope to find a row passes
      if (test) {
        return true;
      } else {
        anyFailing = true;
      }
    }

    // reaching here means either that no row matches the filter path
    // or we had failing rows
    return !anyFailing;
  }

  /**
   * Test a single row against this expression. This method will return <code>true</code> if any of:
   *
   * <ol>
   *   <li>This expression filter path is not matching the path row represents
   *   <li>The condition test returns true
   * </ol>
   */
  public boolean test(RowWrapper row) {
    // if does not match the filter path, we can not test
    if (!matchesFilterPath(row)) {
      return true;
    }

    // otherwise delegate to the condition
    return getCondition().test(row);
  }

  // if given row matches the filter path in the
  public boolean matchesFilterPath(RowWrapper row) {
    List<String> targetPath = getFilterPath().getPath();
    DocumentProperties properties = getCondition().documentProperties();
    return DocsApiUtils.isRowMatchingPath(row, targetPath, properties);
  }

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
    throw new UnsupportedOperationException("FilterExpression does not work with the vars.");
  }

  @Override
  public String toString() {
    return getDescription();
  }
}
