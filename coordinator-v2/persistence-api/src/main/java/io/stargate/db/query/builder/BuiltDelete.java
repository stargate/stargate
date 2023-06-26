package io.stargate.db.query.builder;

import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundDelete;
import io.stargate.db.query.Condition;
import io.stargate.db.query.Modification;
import io.stargate.db.query.Query;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.RowsImpacted;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Table;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class BuiltDelete extends BuiltDML<BuiltDelete.Bound> {
  private final boolean ifExists;

  protected BuiltDelete(
      Table table,
      Codec codec,
      AsyncQueryExecutor executor,
      QueryStringBuilder builder,
      List<BuiltCondition> where,
      List<ValueModifier> modifiers,
      boolean ifExists,
      List<BuiltCondition> conditions,
      Value<Long> timestampValue) {
    super(
        QueryType.DELETE,
        table,
        codec,
        executor,
        builder,
        where,
        modifiers,
        conditions,
        null,
        timestampValue);
    this.ifExists = ifExists;
  }

  private BuiltDelete(
      Table table,
      Codec codec,
      @Nullable MD5Digest preparedId,
      AsyncQueryExecutor executor,
      List<BindMarker> externalBindMarkers,
      DMLData data,
      boolean ifExists) {
    super(QueryType.DELETE, table, codec, preparedId, executor, externalBindMarkers, data);
    this.ifExists = ifExists;
  }

  @Override
  protected BuiltDelete.Bound createBoundQuery(BoundInfo info) {
    return new Bound(
        this,
        info.boundedValues(),
        info.internalBoundValues(),
        info.rowsUpdated(),
        info.modifications(),
        ifExists,
        info.conditions(),
        info.timestamp());
  }

  @Override
  public Query<Bound> withPreparedId(MD5Digest preparedId) {
    return new BuiltDelete(
        table(), valueCodec(), preparedId, executor(), bindMarkers(), data, ifExists);
  }

  public static class Bound extends AbstractBoundDMLWithCondition implements BoundDelete {
    private Bound(
        BuiltDelete builtDelete,
        List<TypedValue> boundedValues,
        List<TypedValue> values,
        RowsImpacted rowsUpdated,
        List<Modification> modifications,
        boolean ifExists,
        List<Condition> conditions,
        OptionalLong timestamp) {
      super(
          builtDelete,
          boundedValues,
          values,
          rowsUpdated,
          modifications,
          ifExists,
          conditions,
          OptionalInt.empty(),
          timestamp);
    }
  }
}
