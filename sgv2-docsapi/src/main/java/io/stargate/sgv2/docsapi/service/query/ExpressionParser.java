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

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import com.bpodgursky.jbool_expressions.Not;
import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Streams;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterExpression.Builder;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.condition.ConditionParser;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterHintCode;
import io.stargate.sgv2.docsapi.service.util.DocsApiUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class ExpressionParser {

  private static final String NOT_OPERATOR = "$not";

  private static final String OR_OPERATOR = "$or";

  private static final String AND_OPERATOR = "$and";

  private final ConditionParser conditionParser;
  private final DocumentProperties documentProperties;

  @Inject
  public ExpressionParser(
      ConditionParser predicateProvider, DocumentProperties documentProperties) {
    this.conditionParser = predicateProvider;
    this.documentProperties = documentProperties;
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
      List<String> prependedPath, JsonNode filterJson, boolean numericBooleans) {
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
      List<String> prependedPath, JsonNode filterJson, boolean numericBooleans) {
    if (!filterJson.isObject()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_OBJECT_REQUIRED);
    }

    return parse(
        prependedPath,
        Collections.singletonList(filterJson),
        numericBooleans,
        new AtomicInteger(0));
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
      List<String> prependedPath,
      Iterable<JsonNode> nodes,
      boolean numericBooleans,
      AtomicInteger nextIndex) {
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
        } else if (Objects.equals(NOT_OPERATOR, fieldOrOp)) {
          JsonNode andChildrenNode = next.getValue();
          Not<FilterExpression> negated =
              resolveNot(andChildrenNode, prependedPath, numericBooleans, nextIndex);
          expressions.add(negated);
        } else {
          FilterPath filterPath = getFilterPath(prependedPath, fieldOrOp);
          JsonNode conditions = next.getValue();
          Optional<Double> selectivity = resolveSelectivity(conditions);
          Collection<BaseCondition> fieldConditions =
              conditionParser.getConditions(conditions, numericBooleans);
          validateFieldConditions(filterPath, fieldConditions, selectivity);

          for (BaseCondition fieldCondition : fieldConditions) {
            int index = nextIndex.getAndIncrement();

            Builder builder =
                ImmutableFilterExpression.builder()
                    .filterPath(filterPath)
                    .condition(fieldCondition)
                    .orderIndex(index);

            selectivity.ifPresent(builder::selectivity);

            expressions.add(builder.build());
          }
        }
      }
    }
    return expressions;
  }

  private Optional<Double> resolveSelectivity(JsonNode conditions) {
    //noinspection UnstableApiUsage
    return Streams.stream(conditions.fields())
        .map(
            entry ->
                FilterHintCode.getByRawValue(entry.getKey())
                    .filter(h -> h == FilterHintCode.SELECTIVITY)
                    .map(
                        h -> {
                          JsonNode value = entry.getValue();
                          if (!value.isNumber()) {
                            String msg =
                                String.format(
                                    "Selectivity hint does not support the provided value %s (expecting a number)",
                                    value);
                            throw new ErrorCodeRuntimeException(
                                ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
                          }
                          return value.asDouble();
                        }))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }

  private void validateFieldConditions(
      FilterPath filterPath, Collection<BaseCondition> conditions, Optional<Double> selectivity) {
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

    // If selectivity is explicitly provided, allow at most one condition to avoid ambiguity.
    if (selectivity.isPresent() && conditions.size() > 1) {
      String msg =
          String.format(
              "Specifying multiple filter conditions in the same JSON block with a selectivity "
                  + "hint is not supported. Combine them using \"$and\" to disambiguate. "
                  + "Related field: '%s')",
              filterPath.getField());
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
    }

    if (selectivity.isPresent() && conditions.isEmpty()) {
      String msg =
          String.format(
              "Field '%s' has a selectivity hint but no condition", filterPath.getField());
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
    }
  }

  private Or<FilterExpression> resolveOr(
      JsonNode node, List<String> prependedPath, boolean numericBooleans, AtomicInteger nextIndex) {
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
      JsonNode node, List<String> prependedPath, boolean numericBooleans, AtomicInteger nextIndex) {
    if (!node.isArray()) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_SEARCH_FILTER_INVALID,
          "The $and requires an array json node as value.");
    }

    List<Expression<FilterExpression>> orConditions =
        parse(prependedPath, node, numericBooleans, nextIndex);
    return And.of(orConditions);
  }

  private Not<FilterExpression> resolveNot(
      JsonNode node, List<String> prependedPath, boolean numericBooleans, AtomicInteger nextIndex) {
    if (!node.isObject()) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_SEARCH_FILTER_INVALID,
          "The $not operator requires a json object as value.");
    }

    List<Expression<FilterExpression>> negatedConditions =
        parse(prependedPath, Collections.singletonList(node), numericBooleans, nextIndex);

    if (negatedConditions.size() != 1) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_SEARCH_FILTER_INVALID,
          "The $not operator requires exactly one child expression.");
    }

    return Not.of(negatedConditions.get(0));
  }

  /**
   * Resolves the collection/document prepended path and the field path as specified in the filter
   * query.
   *
   * @param prependedPath Given collection or document path segments
   * @param fieldPath field path given in the filter query, expects dot notation, f.e. <code>
   *     car.name</code>
   * @return FilterPath
   */
  private FilterPath getFilterPath(List<String> prependedPath, String fieldPath) {
    String[] fieldNamePath = DocsApiUtils.PERIOD_PATTERN.split(fieldPath);
    List<String> convertedFieldNamePath =
        Arrays.stream(fieldNamePath)
            .map(p -> DocsApiUtils.convertArrayPath(p, documentProperties.maxArrayLength()))
            .collect(Collectors.toList());

    if (!prependedPath.isEmpty()) {
      List<String> prependedConverted =
          prependedPath.stream()
              .map(path -> DocsApiUtils.convertArrayPath(path, documentProperties.maxArrayLength()))
              .collect(Collectors.toList());

      convertedFieldNamePath.addAll(0, prependedConverted);
    }

    return ImmutableFilterPath.of(convertedFieldNamePath);
  }
}
