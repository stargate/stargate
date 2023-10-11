package io.stargate.sgv2.api.common.cql;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Or;
import java.util.List;

public class ExpressionUtils<K> {

  public static <K> And<K> OrderedAndOf(Expression<K>... expressions) {
    // expression as creation order
    return And.of(expressions, (e1, e2) -> 1);
  }

  public static <K> And<K> OrderedAndOf(List<? extends Expression<K>> expressions) {
    // expression as creation order
    return And.of(expressions.toArray(new Expression[expressions.size()]), (e1, e2) -> 1);
  }

  public static <K> Or<K> OrderedOrOf(List<? extends Expression<K>> expressions) {
    // expression as creation order
    return Or.of(expressions.toArray(new Expression[expressions.size()]), (e1, e2) -> 1);
  }

  public static <K> Or<K> OrderedOrOf(Expression<K>... expressions) {
    // expression as creation order
    return Or.of(expressions, (e1, e2) -> 1);
  }

  public static <K> Expression<K>[] getAsArray(Expression<K>... expressions) {
    return expressions;
  }
}
