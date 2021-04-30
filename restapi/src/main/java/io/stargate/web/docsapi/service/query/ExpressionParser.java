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
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.condition.ConditionParser;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.ws.rs.core.PathSegment;
import org.apache.commons.lang3.StringUtils;

public class ExpressionParser {

  private static final Pattern PERIOD_PATTERN = Pattern.compile("\\.");

  private final ConditionParser conditionProvider;

  public ExpressionParser(ConditionParser predicateProvider) {
    this.conditionProvider = predicateProvider;
  }

  /**
   * Constructs the filter expression for the given filter query represented by the JSON node.
   *
   * <p>This method will {@link #parse(List, JsonNode)} the filter query first and then:
   *
   * <ol>
   *   <li>1. Combine all expression in with And operation
   *   <li>2. Apply {@link RuleSet#simplify(Expression)} operation
   * </ol>
   *
   * @param prependedPath Given collection or document path segments
   * @param filterJson Filter JSON node
   * @return Returns optimized joined {@link Expression<FilterExpression>} for filtering
   */
  public Expression<FilterExpression> constructFilterExpression(
      List<PathSegment> prependedPath, JsonNode filterJson) {
    List<Expression<FilterExpression>> parse = parse(prependedPath, filterJson);

    // if this is empty we can simply return true
    if (parse.isEmpty()) {
      // TODO ISE: should be actually throw exception here or return optional
      return Literal.getTrue();
    }

    // otherwise combine with and and simplify only
    And<FilterExpression> and = And.of(parse);
    return RuleSet.simplify(and);
  }

  /**
   * Parses the root filter json node and returns back the list of the expressions found.
   *
   * @param prependedPath Given collection or document path segments
   * @param filterJson Filter JSON node
   * @return List of all expressions
   */
  public List<Expression<FilterExpression>> parse(
      List<PathSegment> prependedPath, JsonNode filterJson) {
    List<Expression<FilterExpression>> expressions = new ArrayList<>();

    Iterator<Map.Entry<String, JsonNode>> fields = filterJson.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String fieldPath = field.getKey();
      FilterPath filterPath = getFilterPath(prependedPath, fieldPath);
      Collection<BaseCondition> fieldConditions = conditionProvider.getConditions(field.getValue());
      for (BaseCondition fieldCondition : fieldConditions) {
        ImmutableFilterExpression expression =
            ImmutableFilterExpression.of(filterPath, fieldCondition);
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
        Arrays.stream(fieldNamePath).map(this::convertArrayPath).collect(Collectors.toList());

    if (!prependedPath.isEmpty()) {
      List<String> prependedConverted =
          prependedPath.stream()
              .map(
                  pathSeg -> {
                    String path = pathSeg.getPath();
                    return convertArrayPath(path);
                  })
              .collect(Collectors.toList());

      convertedFieldNamePath.addAll(0, prependedConverted);
    }

    return ImmutableFilterPath.of(convertedFieldNamePath);
  }

  // TODO ISE: taken fom document service, best to extract to some kind of static utils
  private String convertArrayPath(String path) {

    // TODO charAt can be faster here?
    if (path.startsWith("[") && path.endsWith("]")) {
      String innerPath = path.substring(1, path.length() - 1);
      int idx = Integer.parseInt(innerPath);
      if (idx > DocumentDB.MAX_ARRAY_LENGTH - 1) {
        String msg = String.format("Max array length of %s exceeded.", DocumentDB.MAX_ARRAY_LENGTH);
        throw new DocumentAPIRequestException(msg);
      }
      return "[" + leftPadTo6(innerPath) + "]";
    }
    return path;
  }

  // TODO ISE: taken fom document service, best to extract to some kind of static utils
  private String leftPadTo6(String value) {
    return StringUtils.leftPad(value, 6, '0');
  }
}
