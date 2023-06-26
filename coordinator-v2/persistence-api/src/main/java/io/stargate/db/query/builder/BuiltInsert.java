package io.stargate.db.query.builder;

import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundInsert;
import io.stargate.db.query.Modification;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.RowsImpacted;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Table;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class BuiltInsert extends BuiltDML<BuiltInsert.Bound> {
  private final boolean ifNotExists;

  BuiltInsert(
      Table table,
      Codec codec,
      AsyncQueryExecutor executor,
      QueryStringBuilder builder,
      List<BuiltCondition> where,
      List<ValueModifier> modifiers,
      boolean ifNotExists,
      Value<Integer> ttlValue,
      Value<Long> timestampValue) {
    super(
        QueryType.INSERT,
        table,
        codec,
        executor,
        builder,
        where,
        modifiers,
        Collections.emptyList(),
        ttlValue,
        timestampValue);
    this.ifNotExists = ifNotExists;
  }

  private BuiltInsert(
      Table table,
      Codec codec,
      @Nullable MD5Digest preparedId,
      AsyncQueryExecutor executor,
      List<BindMarker> externalBindMarkers,
      DMLData data,
      boolean ifNotExists) {
    super(QueryType.INSERT, table, codec, preparedId, executor, externalBindMarkers, data);
    this.ifNotExists = ifNotExists;
  }

  @Override
  protected BuiltInsert.Bound createBoundQuery(BoundInfo info) {
    return new Bound(
        this,
        info.boundedValues(),
        info.internalBoundValues(),
        info.rowsUpdated(),
        info.modifications(),
        ifNotExists,
        info.ttl(),
        info.timestamp());
  }

  @Override
  public BuiltInsert withPreparedId(MD5Digest preparedId) {
    return new BuiltInsert(
        table(), valueCodec(), preparedId, executor(), bindMarkers(), data, ifNotExists);
  }

  public static class Bound extends AbstractBoundDML implements BoundInsert {
    private final boolean ifNotExists;

    private Bound(
        BuiltInsert builtInsert,
        List<TypedValue> boundedValues,
        List<TypedValue> values,
        RowsImpacted rowsUpdated,
        List<Modification> modifications,
        boolean ifNotExists,
        OptionalInt ttl,
        OptionalLong timestamp) {
      super(builtInsert, boundedValues, values, rowsUpdated, modifications, ttl, timestamp);
      this.ifNotExists = ifNotExists;
    }

    @Override
    public boolean ifNotExists() {
      return ifNotExists;
    }
  }
}
