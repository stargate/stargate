package io.stargate.db.query.builder;

import io.stargate.db.query.BoundDMLQueryWithConditions;
import io.stargate.db.query.Condition;
import io.stargate.db.query.Modification;
import io.stargate.db.query.RowsImpacted;
import io.stargate.db.query.TypedValue;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;

class AbstractBoundDMLWithCondition extends AbstractBoundDML
    implements BoundDMLQueryWithConditions {
  private final boolean ifExists;
  private final List<Condition> conditions;

  protected AbstractBoundDMLWithCondition(
      BuiltDML<?> builtQuery,
      List<TypedValue> boundedValues,
      List<TypedValue> values,
      RowsImpacted rowsUpdated,
      List<Modification> modifications,
      boolean ifExists,
      List<Condition> conditions,
      OptionalInt ttl,
      OptionalLong timestamp) {
    super(builtQuery, boundedValues, values, rowsUpdated, modifications, ttl, timestamp);
    this.ifExists = ifExists;
    this.conditions = conditions;
  }

  @Override
  public boolean ifExists() {
    return ifExists;
  }

  @Override
  public List<Condition> conditions() {
    return conditions;
  }
}
