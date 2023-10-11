package io.stargate.sgv2.api.common.cql.Expression;

import java.util.*;

public class ExpressionUtil {

  @SuppressWarnings("unchecked")
  public static <K> List<Expression<K>> list(Expression... expressions) {
    return Arrays.<Expression<K>>asList(expressions);
  }

  private static <K> Expression<K>[] toArray(Set<Expression<K>> andTerms) {
    int i = 0;
    Expression<K>[] result = expr(andTerms.size());
    for (Expression<K> expr : andTerms) {
      result[i++] = expr;
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public static <K> Expression<K>[] expr(int length) {
    return new Expression[length];
  }

  public static <K> void addAll(Collection<Expression<K>> set, Expression<K>[] values) {
    Collections.addAll(set, values);
  }
}
