package io.stargate.sgv2.api.common.cql.Expression;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Or<K> extends NExpression<K> {

  public static final String EXPR_TYPE = "or";

  private Or(Expression<K>[] children) {
    super(children);
  }

  public static <K> Or<K> of(Expression<K> child1, Expression<K> child2, Expression<K> child3) {
    return of(ExpressionUtil.<K>list(child1, child2, child3));
  }

  public static <K> Or<K> of(Expression<K> child1, Expression<K> child2) {
    return of(ExpressionUtil.<K>list(child1, child2));
  }

  public static <K> Or<K> of(Expression<K> child1) {
    return of(ExpressionUtil.<K>list(child1));
  }

  @SafeVarargs
  public static <K> Or<K> of(Expression<K>... children) {
    return new Or<>(children);
  }

  public static <K> Or<K> of(List<? extends Expression<K>> children) {
    return new Or<K>(children.toArray(new Expression[children.size()]));
  }

  @Override
  public String getExprType() {
    return EXPR_TYPE;
  }

  public String toString() {
    return Arrays.stream(expressions)
        .map(Object::toString)
        .collect(Collectors.joining(" | ", "(", ")"));
  }
}
