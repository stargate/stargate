package io.stargate.sgv2.api.common.cql.Expression;

import java.util.Collections;
import java.util.List;

public class Variable<K> extends Expression<K> {

  public static final String EXPR_TYPE = "variable";

  private final K value;

  private Variable(K value) {
    this.value = value;
  }

  public static <K> Variable<K> of(K value) {
    return new Variable<K>(value);
  }

  public K getValue() {
    return value;
  }

  public String toString() {
    return value.toString();
  }

  @Override
  public String getExprType() {
    return EXPR_TYPE;
  }

  public List<Expression<K>> getChildren() {
    return Collections.emptyList();
  }
}
