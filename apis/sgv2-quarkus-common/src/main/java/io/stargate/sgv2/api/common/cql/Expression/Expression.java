package io.stargate.sgv2.api.common.cql.Expression;

import java.io.Serializable;
import java.util.List;

public abstract class Expression<K> implements Serializable {

  public abstract String getExprType();

  public abstract List<Expression<K>> getChildren();
}
