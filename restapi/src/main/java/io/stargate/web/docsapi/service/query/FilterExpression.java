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
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
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
  public boolean test(List<Row> documentRows) {
    boolean anyFailing = false;
    for (Row row : documentRows) {

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
  public boolean test(Row row) {
    // if does not match the filter path, we can not test
    if (!matchesFilterPath(row)) {
      return true;
    }

    // otherwise delegate to the condition
    return getCondition().test(row);
  }

  // if given row matches the filter path in the
  public boolean matchesFilterPath(Row row) {
    List<String> targetPath = getFilterPath().getPath();
    return DocsApiUtils.isRowMatchingPath(row, targetPath);
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
}
