package io.stargate.web.docsapi.service.filter;

import com.google.common.collect.ImmutableSet;
import io.stargate.db.datastore.query.WhereCondition;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public enum FilterOp {
  EQ("==", WhereCondition.Predicate.Eq, "$eq"),
  LT("<", WhereCondition.Predicate.Lt, "$lt"),
  LTE("<=", WhereCondition.Predicate.Lte, "$lte"),
  GT(">", WhereCondition.Predicate.Gt, "$gt"),
  GTE(">=", WhereCondition.Predicate.Gte, "$gte"),
  EXISTS("==", WhereCondition.Predicate.Eq, "$exists"),

  // These are "limited support" filters because C* doesn't support them natively
  IN("in", null, "$in"), // CQL 3.4.5 doesn't support IN fully
  NE("!=", null, "$ne"), // CQL 3.4.5 doesn't support <>
  NIN("nin", null, "$nin"); // CQL 3.4.5 doesn't support NOT IN

  public final String cqlOp;
  public final WhereCondition.Predicate predicate;
  public final String rawValue;
  public static final Set<FilterOp> LIMITED_SUPPORT_FILTERS =
      ImmutableSet.of(FilterOp.NE, FilterOp.IN, FilterOp.NIN);

  FilterOp(String name, WhereCondition.Predicate predicate, String rawValue) {
    this.cqlOp = name;
    this.predicate = predicate;
    this.rawValue = rawValue;
  }

  public static List<String> allRawValues() {
    return Arrays.stream(FilterOp.values()).map(op -> op.rawValue).collect(Collectors.toList());
  }

  public static Optional<FilterOp> getByRawValue(String rawValue) {
    return Arrays.stream(FilterOp.values()).filter(op -> op.rawValue.equals(rawValue)).findFirst();
  }
}
