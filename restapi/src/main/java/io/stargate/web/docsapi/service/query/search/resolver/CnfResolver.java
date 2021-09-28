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
import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.options.ExprOptions;
import com.bpodgursky.jbool_expressions.rules.Rule;
import com.bpodgursky.jbool_expressions.rules.RuleList;
import com.bpodgursky.jbool_expressions.rules.RulesHelper;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimaps;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.rules.TrueFilterExpressions;
import io.stargate.web.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.filter.impl.InMemoryCandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.filter.impl.PersistenceCandidatesFilter;
import io.stargate.web.docsapi.service.query.search.resolver.impl.AllFiltersResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.AnyFiltersResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.InMemoryDocumentsResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.OrExpressionDocumentsResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.PersistenceDocumentsResolver;
import io.stargate.web.docsapi.service.query.search.weigth.ExpressionWeightResolver;
import io.stargate.web.docsapi.service.query.search.weigth.impl.UserOrderWeightResolver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
   * @param config {@link DocsApiConfiguration}
   * @return DocumentsResolver
   */
  public static DocumentsResolver resolve(
      Expression<FilterExpression> expression,
      ExecutionContext context,
      DocsApiConfiguration config) {
    return resolve(expression, context, null, config);
  }

  /**
   * Returns a document resolver for a single {@link And} expression in CNF form.
   *
   * @param expression {@link FilterExpression}
   * @param context {@link ExecutionContext}
   * @param parent parent resolver or <code>null</code>
   * @param config {@link DocsApiConfiguration}
   * @return DocumentsResolver
   */
  public static DocumentsResolver resolve(
      Expression<FilterExpression> expression,
      ExecutionContext context,
      DocumentsResolver parent,
      DocsApiConfiguration config) {
    // from the children inside and
    And<FilterExpression> andExpression = (And<FilterExpression>) expression;
    List<Expression<FilterExpression>> children = andExpression.getChildren();

    UserOrderWeightResolver weightResolver = UserOrderWeightResolver.of();

    // try to get the next persistence resolver
    return nextPersistenceResolver(expression, children, weightResolver, context, parent, config)
        // if not persistent ones exists, go for the ORs
        .orElseGet(
            () ->
                nextOrResolver(expression, children, weightResolver, context, parent, config)
                    // if this is not working, go for the memory
                    .orElseGet(
                        () ->
                            nextInMemoryResolver(
                                    expression, children, weightResolver, context, parent, config)
                                .orElseThrow(
                                    () ->
                                        // this should never happen
                                        new ErrorCodeRuntimeException(
                                            ErrorCode.DOCS_API_SEARCH_EXPRESSION_NOT_RESOLVED))));
  }

  private static Optional<DocumentsResolver> nextPersistenceResolver(
      Expression<FilterExpression> root,
      List<Expression<FilterExpression>> children,
      ExpressionWeightResolver<FilterExpression> weightResolver,
      ExecutionContext context,
      DocumentsResolver parent,
      DocsApiConfiguration config) {

    // find available and next most important set of persistence expressions
    // these would be the ones on the same filter path
    return allPersistenceExpressions(children)
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
                DocumentsResolver current =
                    new PersistenceDocumentsResolver(selected, context, config);
                // then simplify root
                Expression<FilterExpression> simplified =
                    simplifyCnfExpression(root, ImmutableList.copyOf(selected));
                // and resolve further
                return BaseResolver.resolve(simplified, context, current, config);
              } else {
                // if we have candidates, then do all memory filters at once
                List<Function<ExecutionContext, CandidatesFilter>> all =
                    indexByFilterPath(nextExpressions).asMap().values().stream()
                        .map(value -> PersistenceCandidatesFilter.forExpressions(value, config))
                        .collect(Collectors.toList());
                DocumentsResolver current = new AllFiltersResolver(all, context, parent);

                // then simplify root
                Expression<FilterExpression> simplified =
                    simplifyCnfExpression(root, ImmutableList.copyOf(nextExpressions));

                // and resolve further
                return BaseResolver.resolve(simplified, context, current, config);
              }
            });
  }

  private static Optional<DocumentsResolver> nextInMemoryResolver(
      Expression<FilterExpression> root,
      List<Expression<FilterExpression>> children,
      ExpressionWeightResolver<FilterExpression> weightResolver,
      ExecutionContext context,
      DocumentsResolver parent,
      DocsApiConfiguration config) {

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
                DocumentsResolver current =
                    new InMemoryDocumentsResolver(selected, context, config);
                // then simplify root
                Expression<FilterExpression> simplified =
                    simplifyCnfExpression(root, ImmutableList.copyOf(selected));
                // and resolve further
                return BaseResolver.resolve(simplified, context, current, config);
              } else {
                // if we have candidates, then do all memory filters at once
                List<Function<ExecutionContext, CandidatesFilter>> all =
                    indexByFilterPath(inMemoryExpressions).asMap().values().stream()
                        .map(value -> InMemoryCandidatesFilter.forExpressions(value, config))
                        .collect(Collectors.toList());
                DocumentsResolver current = new AllFiltersResolver(all, context, parent);

                // then simplify root
                Expression<FilterExpression> simplified =
                    simplifyCnfExpression(root, ImmutableList.copyOf(inMemoryExpressions));
                // and resolve further
                return BaseResolver.resolve(simplified, context, current, config);
              }
            });
  }

  private static Optional<DocumentsResolver> nextOrResolver(
      Expression<FilterExpression> root,
      List<Expression<FilterExpression>> children,
      ExpressionWeightResolver<FilterExpression> weightResolver,
      ExecutionContext context,
      DocumentsResolver parent,
      DocsApiConfiguration config) {

    // find next OR expression
    return nextOrExpression(children, weightResolver)
        .map(
            or -> {
              // simplify root
              Expression<FilterExpression> simplified =
                  simplifyCnfExpression(root, Collections.singletonList(or));

              // if we don't have a parent, then we can not filter with candidates
              // take best one
              if (null == parent) {
                // construct current
                DocumentsResolver current = new OrExpressionDocumentsResolver(or, context, config);

                // and resolve further
                return BaseResolver.resolve(simplified, context, current, config);
              } else {
                // collect all children
                Set<FilterExpression> expressions = new HashSet<>();
                or.collectK(expressions, Integer.MAX_VALUE);

                // map to the correct filters
                List<Function<ExecutionContext, CandidatesFilter>> input =
                    expressions.stream()
                        .map(
                            exp -> {
                              if (exp.getCondition().isPersistenceCondition()) {
                                return PersistenceCandidatesFilter.forExpression(exp, config);
                              } else {
                                return InMemoryCandidatesFilter.forExpression(exp, config);
                              }
                            })
                        .collect(Collectors.toList());

                // create the resolver
                AnyFiltersResolver current = new AnyFiltersResolver(input, context, parent);

                // and resolve further
                return BaseResolver.resolve(simplified, context, current, config);
              }
            });
  }

  /** Simplifies by having all resolved ones replaced by {@link Literal#getTrue()} */
  private static Expression<FilterExpression> simplifyCnfExpression(
      Expression<FilterExpression> root, Collection<Expression<FilterExpression>> resolved) {
    List<Rule<?, FilterExpression>> rules = new ArrayList<>();
    rules.add(new TrueFilterExpressions(resolved::contains));
    rules.addAll(RulesHelper.<FilterExpression>simplifyRules().getRules());

    RuleList<FilterExpression> ruleList = new RuleList<>(rules);
    return RulesHelper.applyAll(root, ruleList, ExprOptions.noCaching());
  }

  /** Finds all expression with persistence conditions. */
  private static Optional<Collection<FilterExpression>> allPersistenceExpressions(
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

  /**
   * Finds next OR expression that should be executed. Favors the ones that have the best filters
   * based on the #weightResolver.
   */
  private static Optional<Or<FilterExpression>> nextOrExpression(
      List<Expression<FilterExpression>> children,
      ExpressionWeightResolver<FilterExpression> weightResolver) {
    List<Or<FilterExpression>> ors = getOrExpressions(children);

    if (ors.isEmpty()) {
      return Optional.empty();
    } else {
      return ors.stream()
          .min(
              (or1, or2) -> {
                List<FilterExpression> first =
                    getFilterExpressions(or1.getChildren(), filterExpression -> true);
                List<FilterExpression> second =
                    getFilterExpressions(or2.getChildren(), filterExpression -> true);
                return weightResolver.compare(first, second);
              });
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

  /** Extracts only {@link Or<FilterExpression>} from collection of {@link Expression}s. */
  private static List<Or<FilterExpression>> getOrExpressions(
      List<Expression<FilterExpression>> children) {
    List<?> result =
        children.stream()
            .filter(Or.class::isInstance)
            .map(Or.class::cast)
            .collect(Collectors.toList());

    return (List<Or<FilterExpression>>) result;
  }
}
