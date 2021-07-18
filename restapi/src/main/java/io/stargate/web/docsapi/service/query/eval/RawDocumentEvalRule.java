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

package io.stargate.web.docsapi.service.query.eval;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.eval.EvalRule;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.FilterExpression;
import java.util.List;
import java.util.Map;

/**
 * Evaluates a single FilterExpression against set of rows. Note that this {@link EvalRule} expects
 * all rows of a document to be provided.
 */
public class RawDocumentEvalRule extends EvalRule<FilterExpression> {

  private final List<Row> rows;

  public RawDocumentEvalRule(RawDocument document) {
    this(document.rows());
  }

  public RawDocumentEvalRule(List<Row> rows) {
    this.rows = rows;
  }

  /** {@inheritDoc} */
  @Override
  public boolean evaluate(
      Expression<FilterExpression> expression, Map<String, EvalRule<FilterExpression>> rules) {
    FilterExpression filterExpression = (FilterExpression) expression;

    // check if any row matches the filter path and if we should evaluate on missing
    boolean anyOnPath = rows.stream().anyMatch(filterExpression::matchesFilterPath);
    boolean evaluateOnMissingFields = filterExpression.getCondition().isEvaluateOnMissingFields();

    // if so do a test, otherwise return false, that means that no row matches the filter path and
    // condition is not eval on missing
    if (anyOnPath || evaluateOnMissingFields) {
      return filterExpression.test(rows);
    } else {
      return false;
    }
  }
}
