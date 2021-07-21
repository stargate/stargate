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

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import com.bpodgursky.jbool_expressions.options.ExprOptions;
import com.bpodgursky.jbool_expressions.rules.Rule;
import com.bpodgursky.jbool_expressions.rules.RuleList;
import com.bpodgursky.jbool_expressions.rules.RulesHelper;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.rules.TrueFilterExpressions;
import io.stargate.web.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.filter.impl.InMemoryCandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.filter.impl.PersistenceCandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.impl.AllFiltersResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.InMemoryDocumentsResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.PersistenceDocumentsResolver;
import io.stargate.web.docsapi.service.query.search.weigth.ExpressionWeightResolver;
import io.stargate.web.docsapi.service.query.search.weigth.impl.UserOrderWeightResolver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Static resolver that accepts the {@link And} expression in the CNF form. */
public final class CnfResolver {

  private CnfResolver() {}

  /**
   * Returns a document resolver for a single {@link And} expression in CNF form without a parent.
   *
   * @param expression {@link FilterExpression}
   * @param context {@link ExecutionContext}
   * @return DocumentsResolver
   */
  public static DocumentsResolver resolve(
      Expression<FilterExpression> expression, ExecutionContext context) {
    return resolve(expression, context, null);
  }

  /**
   * Returns a document resolver for a single {@link And} expression in CNF form.
   *
   * @param expression {@link FilterExpression}
   * @param context {@link ExecutionContext}
   * @param parent parent resolver or <code>null</code>
   * @return DocumentsResolver
   */
  public static DocumentsResolver resolve(
      Expression<FilterExpression> expression, ExecutionContext context, DocumentsResolver parent) {
    // from the children inside and
    And<FilterExpression> andExpression = (And<FilterExpression>) expression;
    List<Expression<FilterExpression>> children = andExpression.getChildren();

    UserOrderWeightResolver weightResolver = UserOrderWeightResolver.of();

    // try to get the next persistence resolver
    return nextPersistenceResolver(expression, children, weightResolver, context, parent)
        .orElseGet(
            () ->
                // if this is not working, go for the memory
                nextInMemoryResolver(expression, children, weightResolver, context, parent)
                    .orElseThrow(
                        () ->
                            // this should happen only if we have ors
                            new ErrorCodeRuntimeException(
                                ErrorCode.DOCS_API_SEARCH_OR_NOT_SUPPORTED)));
  }

  private static Optional<DocumentsResolver> nextPersistenceResolver(
      Expression<FilterExpression> root,
      List<Expression<FilterExpression>> children,
      ExpressionWeightResolver<FilterExpression> weightResolver,
      ExecutionContext context,
      DocumentsResolver parent) {

    // find available and next most important set of persistence expressions
    // these would be the ones on the same filter path
    return nextPersistenceExpressions(children)
        .map(
            nextExpressions -> {
              // if we don't have a parent, then we can not filter with candidates
              // take best one
              if (null == parent) {
                Collection<FilterExpression> selected =
                    indexByFilterPath(nextExpressions).asMap().values().stream()
                        .reduce((ec1, ec2) -> weightResolver.collection().apply(ec1, ec2))
                        .orElseThrow(
                            () -> new IllegalArgumentException("No persistence expressions."));

                // construct current
                DocumentsResolver current = new PersistenceDocumentsResolver(selected, context);
                // then simplify root
                Expression<FilterExpression> simplified = simplifyCnfExpression(root, selected);
                // and resolve further
                return BaseResolver.resolve(simplified, context, current);
              } else {
                // if we have candidates, then do all memory filters at once
                List<Function<ExecutionContext, CandidatesFilter>> all =
                    indexByFilterPath(nextExpressions).asMap().values().stream()
                        .map(PersistenceCandidatesFilter::forExpressions)
                        .collect(Collectors.toList());
                DocumentsResolver current = new AllFiltersResolver(all, context, parent);

                // then simplify root
                Expression<FilterExpression> simplified =
                    simplifyCnfExpression(root, nextExpressions);

                // and resolve further
                return BaseResolver.resolve(simplified, context, current);
              }
            });
  }

  private static Optional<DocumentsResolver> nextInMemoryResolver(
      Expression<FilterExpression> root,
      List<Expression<FilterExpression>> children,
      ExpressionWeightResolver<FilterExpression> weightResolver,
      ExecutionContext context,
      DocumentsResolver parent) {

    // find all available in memory expressions
    return allInMemoryExpressionExpressions(children)
        .map(
            inMemoryExpressions -> {
              // if we don't have a parent, then we can not filter with candidates
              // take best one
              if (null == parent) {
                Collection<FilterExpression> selected =
                    indexByFilterPath(inMemoryExpressions).asMap().values().stream()
                        .reduce((ec1, ec2) -> weightResolver.collection().apply(ec1, ec2))
                        .orElseThrow(() -> new IllegalArgumentException("No memory expressions."));

                // construct current
                DocumentsResolver current = new InMemoryDocumentsResolver(selected, context);
                // then simplify root
                Expression<FilterExpression> simplified = simplifyCnfExpression(root, selected);
                // and resolve further
                return BaseResolver.resolve(simplified, context, current);
              } else {
                // if we have candidates, then do all memory filters at once
                List<Function<ExecutionContext, CandidatesFilter>> all =
                    indexByFilterPath(inMemoryExpressions).asMap().values().stream()
                        .map(InMemoryCandidatesFilter::forExpressions)
                        .collect(Collectors.toList());
                DocumentsResolver current = new AllFiltersResolver(all, context, parent);

                // then simplify root
                Expression<FilterExpression> simplified =
                    simplifyCnfExpression(root, inMemoryExpressions);
                // and resolve further
                return BaseResolver.resolve(simplified, context, current);
              }
            });
  }

  /** Simplifies by having all resolved ones replaced by {@link Literal#getTrue()} */
  private static Expression<FilterExpression> simplifyCnfExpression(
      Expression<FilterExpression> root, Collection<FilterExpression> resolved) {
    List<Rule<?, FilterExpression>> rules = new ArrayList<>();
    rules.add(new TrueFilterExpressions(resolved::contains));
    rules.addAll(RulesHelper.<FilterExpression>simplifyRules().getRules());

    RuleList<FilterExpression> ruleList = new RuleList<>(rules);
    return RulesHelper.applyAll(root, ruleList, ExprOptions.noCaching());
  }

  /** Finds all expression with persistence conditions. */
  private static Optional<Collection<FilterExpression>> nextPersistenceExpressions(
      List<Expression<FilterExpression>> children) {

    // collect
    List<FilterExpression> persistenceExpressions =
        getFilterExpressions(children, e -> e.getCondition().isPersistenceCondition());

    // if not available return empty
    if (persistenceExpressions.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(persistenceExpressions);
    }
  }

  /** Finds all expression with in-memory conditions. */
  private static Optional<Collection<FilterExpression>> allInMemoryExpressionExpressions(
      List<Expression<FilterExpression>> children) {
    List<FilterExpression> inMemoryExpressions =
        getFilterExpressions(children, e -> !e.getCondition().isPersistenceCondition());

    if (inMemoryExpressions.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(inMemoryExpressions);
    }
  }

  /** index the expressions by filter path */
  private static Multimap<FilterPath, FilterExpression> indexByFilterPath(
      Iterable<FilterExpression> persistenceExpressions) {
    return Multimaps.index(persistenceExpressions, FilterExpression::getFilterPath);
  }

  /** Extracts only {@link FilterExpression} from collection of {@link Expression}s. */
  private static List<FilterExpression> getFilterExpressions(
      List<Expression<FilterExpression>> children, Predicate<FilterExpression> predicate) {
    return children.stream()
        .filter(FilterExpression.class::isInstance)
        .map(FilterExpression.class::cast)
        .filter(predicate)
        .collect(Collectors.toList());
  }
}
