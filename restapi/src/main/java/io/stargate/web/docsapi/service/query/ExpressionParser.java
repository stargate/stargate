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
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.ConditionParser;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.core.PathSegment;

public class ExpressionParser {

  private static final Pattern PERIOD_PATTERN = Pattern.compile("\\.");

  private final ConditionParser conditionProvider;

  @Inject
  public ExpressionParser(ConditionParser predicateProvider) {
    this.conditionProvider = predicateProvider;
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
   * Parses the root filter json node and returns back the list of the expressions found.
   *
   * @param prependedPath Given collection or document path segments
   * @param filterJson Filter JSON node
   * @param numericBooleans If number booleans should be used when creating conditions.
   * @return List of all expressions
   */
  public List<Expression<FilterExpression>> parse(
      List<PathSegment> prependedPath, JsonNode filterJson, boolean numericBooleans) {
    List<Expression<FilterExpression>> expressions = new ArrayList<>();

    int index = 0;
    Iterator<Map.Entry<String, JsonNode>> fields = filterJson.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String fieldPath = field.getKey();
      FilterPath filterPath = getFilterPath(prependedPath, fieldPath);
      Collection<BaseCondition> fieldConditions =
          conditionProvider.getConditions(field.getValue(), numericBooleans);
      for (BaseCondition fieldCondition : fieldConditions) {
        ImmutableFilterExpression expression =
            ImmutableFilterExpression.of(filterPath, fieldCondition, index++);
        expressions.add(expression);
      }
    }

    return expressions;
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
