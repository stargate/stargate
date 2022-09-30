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

package io.stargate.sgv2.docsapi.service.query.search.resolver;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.rules.ExpressionUtils;

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
   * @param documentProperties {@link DocumentProperties}
   * @return DocumentsResolver
   */
  public static DocumentsResolver resolve(
      Expression<FilterExpression> expression,
      ExecutionContext context,
      DocumentProperties documentProperties) {
    return resolve(expression, context, null, documentProperties);
  }

  /**
   * Resolves the document resolver with optional parent.
   *
   * @param expression {@link Expression}
   * @param parent parent or <code>null</code>
   * @param documentProperties {@link DocumentProperties}
   * @return DocumentsResolver
   */
  public static DocumentsResolver resolve(
      Expression<FilterExpression> expression,
      ExecutionContext context,
      DocumentsResolver parent,
      DocumentProperties documentProperties) {
    // if we are hitting the literal TRUE, then return parent
    if (Literal.EXPR_TYPE.equals(expression.getExprType())) {
      return parent;
    }

    // execute cnf optimization
    Expression<FilterExpression> cnf = ExpressionUtils.toSimplifiedCnf(expression);

    // since this will simplify as well, check if we have And
    // if we have And proceed to the CNF resolver
    if (And.EXPR_TYPE.equals(cnf.getExprType())) {
      return CnfResolver.resolve(cnf, context, parent, documentProperties);
    } else {
      // otherwise wrap to AND, and forward to the CNF
      return CnfResolver.resolve(And.of(cnf), context, parent, documentProperties);
    }
  }
}
