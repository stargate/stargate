package io.stargate.db.query.builder;

import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundUpdate;
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

public class BuiltUpdate extends BuiltDML<BuiltUpdate.Bound> {

  private final boolean ifExists;

  protected BuiltUpdate(
      Table table,
      Codec codec,
      AsyncQueryExecutor executor,
      QueryStringBuilder builder,
      List<BuiltCondition> where,
      List<ValueModifier> modifiers,
      boolean ifExists,
      List<BuiltCondition> conditions,
      Value<Integer> ttlValue,
      Value<Long> timestampValue) {
    super(
        QueryType.UPDATE,
        table,
        codec,
        executor,
        builder,
        where,
        modifiers,
        conditions,
        ttlValue,
        timestampValue);
    this.ifExists = ifExists;
  }

  private BuiltUpdate(
      Table table,
      Codec codec,
      @Nullable MD5Digest preparedId,
      AsyncQueryExecutor executor,
      List<BindMarker> externalBindMarkers,
      DMLData data,
      boolean ifExists) {
    super(QueryType.UPDATE, table, codec, preparedId, executor, externalBindMarkers, data);
    this.ifExists = ifExists;
  }

  @Override
  protected BuiltUpdate.Bound createBoundQuery(BoundInfo info) {
    RowsImpacted rowsUpdated = info.rowsUpdated();

    return new BuiltUpdate.Bound(
        this,
        info.boundedValues(),
        info.internalBoundValues(),
        rowsUpdated,
        info.modifications(),
        ifExists,
        info.conditions(),
        info.ttl(),
        info.timestamp());
  }

  @Override
  public Query<Bound> withPreparedId(MD5Digest preparedId) {
    return new BuiltUpdate(
        table(), valueCodec(), preparedId, executor(), bindMarkers(), data, ifExists);
  }

  public static class Bound extends AbstractBoundDMLWithCondition implements BoundUpdate {
    private Bound(
        BuiltUpdate builtUpdate,
        List<TypedValue> boundedValues,
        List<TypedValue> values,
        RowsImpacted rowsUpdated,
        List<Modification> modifications,
        boolean ifExists,
        List<Condition> conditions,
        OptionalInt ttl,
        OptionalLong timestamp) {
      super(
          builtUpdate,
          boundedValues,
          values,
          rowsUpdated,
          modifications,
          ifExists,
          conditions,
          ttl,
          timestamp);
    }
  }
}
