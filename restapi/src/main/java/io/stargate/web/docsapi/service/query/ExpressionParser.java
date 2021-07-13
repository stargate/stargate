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

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.ConditionParser;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.core.PathSegment;
import org.apache.commons.lang3.mutable.MutableInt;

public class ExpressionParser {

  private static final String OR_OPERATOR = "$or";

  private static final String AND_OPERATOR = "$and";

  private static final Pattern PERIOD_PATTERN = Pattern.compile("\\.");

  private final ConditionParser conditionParser;

  @Inject
  public ExpressionParser(ConditionParser predicateProvider) {
    this.conditionParser = predicateProvider;
  }

  /**
   * Constructs the filter expression for the given filter query represented by the JSON node.
   *
   * <p>This method will {@link #parse(List, JsonNode, boolean)} the filter query first and then:
   *
   * <ol>
   *   <li>1. Combine all expression in with And operation
   *   <li>2. Apply {@link RuleSet#simplify(Expression)} operation
   * </ol>
   *
   * @param prependedPath Given collection or document path segments
   * @param filterJson Filter JSON node
   * @param numericBooleans If number booleans should be used when creating conditions.
   * @return Returns optimized joined {@link Expression<FilterExpression>} for filtering
   */
  public Expression<FilterExpression> constructFilterExpression(
      List<PathSegment> prependedPath, JsonNode filterJson, boolean numericBooleans) {
    List<Expression<FilterExpression>> parse = parse(prependedPath, filterJson, numericBooleans);

    // if this is empty we can simply return true
    if (parse.isEmpty()) {
      return Literal.getTrue();
    }

    // otherwise combine with and simplify only
    And<FilterExpression> and = And.of(parse);
    return RuleSet.simplify(and);
  }

  /**
   * Parses the root filter json node and returns back the list of the expressions found on the
   * first level.
   *
   * @param prependedPath Given collection or document path segments
   * @param filterJson Filter JSON node
   * @param numericBooleans If number booleans should be used when creating conditions.
   * @return List of all expressions
   */
  private List<Expression<FilterExpression>> parse(
      List<PathSegment> prependedPath, JsonNode filterJson, boolean numericBooleans) {
    return parse(
        prependedPath, Collections.singletonList(filterJson), numericBooleans, new MutableInt(0));
  }

  /**
   * Parses set of json nodes and returns back the list of the expressions found. Note that
   * expression in the list contain only first level conditions in each node.
   *
   * @param prependedPath Given collection or document path segments
   * @param nodes JSON nodes to resolve
   * @param numericBooleans If number booleans should be used when creating conditions.
   * @param nextIndex index counter for the expressions
   * @return List of all expressions
   */
  private List<Expression<FilterExpression>> parse(
      List<PathSegment> prependedPath,
      Iterable<JsonNode> nodes,
      boolean numericBooleans,
      MutableInt nextIndex) {
    List<Expression<FilterExpression>> expressions = new ArrayList<>();

    // go through all the nodes given
    for (JsonNode node : nodes) {
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

      // until we have more fields left
      while (fields.hasNext()) {
        // get field
        Map.Entry<String, JsonNode> next = fields.next();
        String fieldOrOp = next.getKey();

        // if $or go for Or operator
        // if $and go for And operator
        // otherwise go for the field resolving
        if (Objects.equals(OR_OPERATOR, fieldOrOp)) {
          JsonNode orChildrenNode = next.getValue();
          Or<FilterExpression> or =
              resolveOr(orChildrenNode, prependedPath, numericBooleans, nextIndex);
          expressions.add(or);
        } else if (Objects.equals(AND_OPERATOR, fieldOrOp)) {
          JsonNode andChildrenNode = next.getValue();
          And<FilterExpression> and =
              resolveAnd(andChildrenNode, prependedPath, numericBooleans, nextIndex);
          expressions.add(and);
        } else {
          FilterPath filterPath = getFilterPath(prependedPath, fieldOrOp);
          Collection<BaseCondition> fieldConditions =
              conditionParser.getConditions(next.getValue(), numericBooleans);
          validateFieldConditions(filterPath, fieldConditions);
          for (BaseCondition fieldCondition : fieldConditions) {
            ImmutableFilterExpression expression =
                ImmutableFilterExpression.of(
                    filterPath, fieldCondition, nextIndex.getAndIncrement());
            expressions.add(expression);
          }
        }
      }
    }

    return expressions;
  }

  private void validateFieldConditions(
      FilterPath filterPath, Collection<BaseCondition> conditions) {
    // If some conditions imply specific and different value types it is a user error since each
    // field can have a value of only one type at a time.
    // Example: `field GT 1` and `field EQ "string"`
    Set<? extends Class<?>> specificTypes =
        conditions.stream()
            .map(BaseCondition::getQueryValueType)
            .filter(c -> Object.class != c)
            .collect(Collectors.toSet());
    if (specificTypes.size() > 1) {
      String msg =
          String.format(
              "Filter conditions for field '%s' imply incompatible types: %s",
              filterPath.getField(),
              specificTypes.stream().map(Class::getSimpleName).collect(Collectors.joining(", ")));
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
    }
  }

  private Or<FilterExpression> resolveOr(
      JsonNode node,
      List<PathSegment> prependedPath,
      boolean numericBooleans,
      MutableInt nextIndex) {
    if (!node.isArray()) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_SEARCH_FILTER_INVALID,
          "The $or requires an array json node as value.");
    }

    List<Expression<FilterExpression>> orConditions =
        parse(prependedPath, node, numericBooleans, nextIndex);
    return Or.of(orConditions);
  }

  private And<FilterExpression> resolveAnd(
      JsonNode node,
      List<PathSegment> prependedPath,
      boolean numericBooleans,
      MutableInt nextIndex) {
    if (!node.isArray()) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_SEARCH_FILTER_INVALID,
          "The $and requires an array json node as value.");
    }

    List<Expression<FilterExpression>> orConditions =
        parse(prependedPath, node, numericBooleans, nextIndex);
    return And.of(orConditions);
  }

  /**
   * Resolves the collection/document prepended path and the filed path as specified in the filter
   * query.
   *
   * @param prependedPath Given collection or document path segments
   * @param fieldPath filed path given in the filter query, expects dot notation, f.e. <code>
   *     car.name</code>
   * @return FilterPath
   */
  private FilterPath getFilterPath(List<PathSegment> prependedPath, String fieldPath) {
    String[] fieldNamePath = PERIOD_PATTERN.split(fieldPath);
    List<String> convertedFieldNamePath =
        Arrays.stream(fieldNamePath)
            .map(DocsApiUtils::convertArrayPath)
            .collect(Collectors.toList());

    if (!prependedPath.isEmpty()) {
      List<String> prependedConverted =
          prependedPath.stream()
              .map(
                  pathSeg -> {
                    String path = pathSeg.getPath();
                    return DocsApiUtils.convertArrayPath(path);
                  })
              .collect(Collectors.toList());

      convertedFieldNamePath.addAll(0, prependedConverted);
    }

    return ImmutableFilterPath.of(convertedFieldNamePath);
  }
}
