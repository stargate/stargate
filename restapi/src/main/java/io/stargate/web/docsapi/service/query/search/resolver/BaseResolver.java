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

package io.stargate.web.docsapi.service.query.search.resolver;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.query.FilterExpression;

/**
 * Base resolver knows what {@link DocumentsResolver} should be created for the given {@link
 * Expression}.
 */
public final class BaseResolver {

  private BaseResolver() {}

  /**
   * Resolves the document resolver without any parent.
   *
   * @param expression {@link Expression}
   * @return DocumentsResolver
   */
  public static DocumentsResolver resolve(
      Expression<FilterExpression> expression, ExecutionContext context) {
    return resolve(expression, context, null);
  }

  /**
   * Resolves the document resolver with optional parent.
   *
   * @param expression {@link Expression}
   * @param parent parent or <code>null</code>
   * @return DocumentsResolver
   */
  public static DocumentsResolver resolve(
      Expression<FilterExpression> expression, ExecutionContext context, DocumentsResolver parent) {

    // if we are hitting the literal TRUE, then return parent
    if (Literal.EXPR_TYPE.equals(expression.getExprType())) {
      return parent;
    }

    // if we have only one Filter expression, then to SingleExpression resolver
    if (FilterExpression.EXPR_TYPE.equals(expression.getExprType())) {
      return SingleExpressionResolver.resolve((FilterExpression) expression, context, parent);
    }

    // for now in every other case go for the CNF resolver
    Expression<FilterExpression> cnfExpression = RuleSet.toCNF(expression);
    return CnfResolver.resolve(cnfExpression, context, parent);
  }
}
