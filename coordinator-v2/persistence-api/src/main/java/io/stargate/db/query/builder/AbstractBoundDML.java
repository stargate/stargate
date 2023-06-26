package io.stargate.db.query.builder;

import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.Modification;
import io.stargate.db.query.RowsImpacted;
import io.stargate.db.query.TypedValue;
import io.stargate.db.schema.Table;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;

class AbstractBoundDML extends AbstractBound<BuiltDML<?>> implements BoundDMLQuery {
  private final RowsImpacted rowsUpdated;
  private final List<Modification> modifications;
  private final OptionalInt ttl;
  private final OptionalLong timestamp;

  protected AbstractBoundDML(
      BuiltDML<?> builtQuery,
      List<TypedValue> boundedValues,
      List<TypedValue> values,
      RowsImpacted rowsUpdated,
      List<Modification> modifications,
      OptionalInt ttl,
      OptionalLong timestamp) {
    super(builtQuery, boundedValues, values);
    this.rowsUpdated = rowsUpdated;
    this.modifications = modifications;
    this.ttl = ttl;
    this.timestamp = timestamp;
  }

  @Override
  public Table table() {
    return source().query().table();
  }

  @Override
  public RowsImpacted rowsUpdated() {
    return rowsUpdated;
  }

  @Override
  public List<Modification> modifications() {
    return modifications;
  }

  @Override
  public OptionalInt ttl() {
    return ttl;
  }

  @Override
  public OptionalLong timestamp() {
    return timestamp;
  }
}
