package io.stargate.sgv2.api.common.cql.Expression;

import java.util.Arrays;
import java.util.List;

public abstract class NExpression<K> extends Expression<K> {

  public final Expression<K>[] expressions;

  protected NExpression(Expression<K>[] expressions) {
    if (expressions.length == 0) {
      throw new IllegalArgumentException("Arguments length 0!");
    }
    this.expressions = Arrays.copyOf(expressions, expressions.length);
  }

  @Override
  public List<Expression<K>> getChildren() {
    if (expressions == null) {
      return null;
    }
    return ExpressionUtil.list(expressions);
  }
}
