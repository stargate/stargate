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

import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.search.resolver.filter.impl.InMemoryCandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.filter.impl.PersistenceCandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.impl.AllFiltersResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.InMemoryDocumentsResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.PersistenceDocumentsResolver;
import java.util.Optional;

/** Static resolver that resolves from the single {@link FilterExpression}. */
public final class SingleExpressionResolver {

  private SingleExpressionResolver() {}

  /**
   * Returns a document resolver for a single {@link FilterExpression} without a parent resolver.
   *
   * @param expression {@link FilterExpression}
   * @param context {@link ExecutionContext}
   * @return DocumentsResolver
   */
  public static DocumentsResolver resolve(FilterExpression expression, ExecutionContext context) {
    return resolve(expression, context, null);
  }

  /**
   * Returns a document resolver for a single {@link FilterExpression}.
   *
   * @param expression {@link FilterExpression}
   * @param context {@link ExecutionContext}
   * @param parent parent resolver or <code>null</code>
   * @return DocumentsResolver
   */
  public static DocumentsResolver resolve(
      FilterExpression expression, ExecutionContext context, DocumentsResolver parent) {
    // make sure parent is respected
    // differentiate between persistence and in memory ones
    boolean persistenceCondition = expression.getCondition().isPersistenceCondition();
    DocumentsResolver resolver;
    if (persistenceCondition) {
      resolver =
          Optional.ofNullable(parent)
              .<DocumentsResolver>map(
                  p ->
                      new AllFiltersResolver(
                          PersistenceCandidatesFilter.forExpression(expression), context, p))
              .orElseGet(() -> new PersistenceDocumentsResolver(expression, context));
    } else {
      resolver =
          Optional.ofNullable(parent)
              .<DocumentsResolver>map(
                  p ->
                      new AllFiltersResolver(
                          InMemoryCandidatesFilter.forExpression(expression), context, p))
              .orElseGet(() -> new InMemoryDocumentsResolver(expression, context));
    }
    return resolver;
  }
}
