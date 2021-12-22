package io.stargate.db.query.builder;

import static com.datastax.oss.driver.shaded.guava.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.Condition;
import io.stargate.db.query.ImmutableCondition;
import io.stargate.db.query.ImmutableModification;
import io.stargate.db.query.ModifiableEntity;
import io.stargate.db.query.Modification;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.RowsImpacted;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Table;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.utils.MD5Digest;

abstract class BuiltDML<Q extends AbstractBound<?> & BoundDMLQuery> extends BuiltQuery<Q> {

  private final Table table;
  protected final DMLData data;

  protected BuiltDML(
      QueryType queryType,
      Table table,
      Codec codec,
      AsyncQueryExecutor executor,
      QueryStringBuilder builder,
      List<BuiltCondition> where,
      List<ValueModifier> modifiers,
      List<BuiltCondition> conditions,
      @Nullable Value<Integer> ttlValue,
      @Nullable Value<Long> timestampValue) {
    this(
        queryType,
        table,
        codec,
        null,
        executor,
        builder.externalBindMarkers(),
        new DMLData(
            builder.externalQueryString(),
            builder.internalBindMarkers(),
            builder.internalQueryString(),
            where,
            modifiers,
            conditions,
            ttlValue,
            timestampValue));
  }

  protected BuiltDML(
      QueryType queryType,
      Table table,
      Codec codec,
      @Nullable MD5Digest preparedId,
      AsyncQueryExecutor executor,
      List<BindMarker> externalBindMarkers,
      DMLData data) {
    super(queryType, codec, preparedId, executor, externalBindMarkers);
    this.table = table;
    this.data = data;
  }

  @Override
  public String queryStringForPreparation() {
    return data.internalQueryString;
  }

  public Table table() {
    return table;
  }

  @Override
  protected Q createBoundQuery(List<TypedValue> values) {
    BoundInfo info = new BoundInfo(this, values);
    info.handleWhereClause();
    info.handleRegularAndStaticModifications();
    info.handleTimestamp();
    info.handleTTL();
    info.handleConditions();
    return createBoundQuery(info);
  }

  protected abstract Q createBoundQuery(BoundInfo builder);

  @Override
  public final String toString() {
    return data.externalQueryString;
  }

  // This class only exists to avoid BuiltDML sub-classes to have to pass all those arguments
  // when creating copy of themselves for their withPreparedId() implementation.
  protected static class DMLData {
    private final String externalQueryString;
    private final int internalBindMarkerCount;
    private final String internalQueryString;
    private final List<BuiltCondition> where;
    private final List<ValueModifier> regularAndStaticModifiers;
    private final List<BuiltCondition> conditions;
    private final @Nullable Value<Integer> ttlValue;
    private final @Nullable Value<Long> timestampValue;

    private DMLData(
        String externalQueryString,
        int internalBindMarkerCount,
        String internalQueryString,
        List<BuiltCondition> where,
        List<ValueModifier> regularAndStaticModifiers,
        List<BuiltCondition> ifConditions,
        @Nullable Value<Integer> ttlValue,
        @Nullable Value<Long> timestampValue) {
      this.externalQueryString = externalQueryString;
      this.internalBindMarkerCount = internalBindMarkerCount;
      this.internalQueryString = internalQueryString;
      this.where = where;
      this.regularAndStaticModifiers = regularAndStaticModifiers;
      this.conditions = ifConditions;
      this.ttlValue = ttlValue;
      this.timestampValue = timestampValue;
    }
  }

  protected static class BoundInfo {
    private final BuiltDML<?> dml;
    private final List<TypedValue> boundValues;
    private final TypedValue[] internalBoundValues;
    private final WhereProcessor whereProcessor;
    private RowsImpacted rowsUpdated;
    private final List<Modification> modifications = new ArrayList<>();
    private final List<Condition> conditions = new ArrayList<>();
    private Integer ttl;
    private Long timestamp;

    BoundInfo(BuiltDML<?> dml, List<TypedValue> boundValues) {
      this.dml = dml;
      this.boundValues = boundValues;
      this.internalBoundValues = new TypedValue[dml.data.internalBindMarkerCount];
      this.whereProcessor =
          new WhereProcessor(dml.table, dml.valueCodec()) {
            @Override
            protected TypedValue handleValue(String name, ColumnType type, Value<?> value) {
              return BoundInfo.this.handleValue(name, type, value);
            }

            @Override
            protected void onNonColumnNameLHS(BuiltCondition.LHS lhs) {
              throw new IllegalArgumentException(
                  format(
                      "Invalid condition %s: cannot have condition on the sub-part of a primary key",
                      lhs));
            }

            @Override
            protected void onNonPrimaryKeyCondition(Column column) {
              throw new IllegalArgumentException(
                  format(
                      "Invalid WHERE condition for DML on non primary key column %s",
                      column.cqlName()));
            }

            @Override
            protected void onInequalityConditionOnPartitionKey(
                Column column, BuiltCondition condition) {
              throw new IllegalArgumentException(
                  format("Invalid condition %s for partition key %s", condition, column.cqlName()));
            }
          };
    }

    List<TypedValue> boundedValues() {
      return boundValues;
    }

    private void handleWhereClause() {
      this.rowsUpdated = whereProcessor.process(dml.data.where);
      if (rowsUpdated == null) {
        // This can only happen if the WHERE clause is a range over multiple partition, which
        // is invalid.
        throw new IllegalArgumentException(
            "Invalid WHERE clause: DML over a range of partitions are not supported");
      }
    }

    private void handleRegularAndStaticModifications() {
      for (ValueModifier modifier : dml.data.regularAndStaticModifiers) {
        Column column = dml.table.existingColumn(modifier.target().columnName());
        ModifiableEntity entity = createEntity(column, modifier.target());
        TypedValue v = handleValue(entity.toString(), entity.type(), modifier.value());
        if (!v.isUnset()) {
          modifications.add(
              ImmutableModification.builder()
                  .entity(entity)
                  .operation(modifier.operation())
                  .value(v)
                  .build());
        }
      }
    }

    private ModifiableEntity createEntity(Column column, ValueModifier.Target target) {
      if (target.fieldName() != null) {
        return ModifiableEntity.udtType(column, target.fieldName());
      } else if (target.mapKey() != null) {
        ColumnType type = column.type();
        checkArgument(type != null, "Provided column does not have its type set");
        checkArgument(
            type.isMap(),
            "Cannot access elements of column %s of type %s in %s, it is not a map",
            column.cqlName(),
            type,
            dml.table.cqlQualifiedName());
        ColumnType keyType = type.parameters().get(0);
        Value<?> key = target.mapKey();
        TypedValue keyValue =
            dml.convertValue(key, "element of " + column.cqlName(), keyType, boundValues);
        checkArgument(
            !keyValue.isUnset(),
            "Cannot use UNSET for map column %s key in table %s",
            column.cqlName(),
            dml.table.cqlQualifiedName());
        return ModifiableEntity.mapValue(column, keyValue);
      } else {
        return ModifiableEntity.of(column);
      }
    }

    private TypedValue handleValue(Column column, Value<?> value) {
      return handleValue(column.cqlName(), column.type(), value);
    }

    private TypedValue handleValue(String entityName, ColumnType type, Value<?> value) {
      TypedValue v = dml.convertValue(value, entityName, type, boundValues);
      int internalIndex = value.internalIndex();
      if (internalIndex >= 0) {
        internalBoundValues[internalIndex] = v;
      }
      return v;
    }

    private void handleTTL() {
      if (dml.data.ttlValue == null) {
        return;
      }
      TypedValue v = handleValue(Column.TTL, dml.data.ttlValue);
      if (!v.isUnset()) {
        ttl = (Integer) v.javaValue();
        if (ttl == null) {
          throw new IllegalArgumentException("Cannot pass null as bound value for the TTL");
        }
      }
    }

    private void handleTimestamp() {
      if (dml.data.timestampValue == null) {
        return;
      }
      TypedValue v = handleValue(Column.TIMESTAMP, dml.data.timestampValue);
      if (!v.isUnset()) {
        timestamp = (Long) v.javaValue();
        if (timestamp == null) {
          throw new IllegalArgumentException("Cannot pass null as bound value for the TIMESTAMP");
        }
      }
    }

    private void handleConditions() {
      for (BuiltCondition bc : dml.data.conditions) {
        Condition.LHS lhs = createLHS(bc.lhs());
        TypedValue v;
        if (bc.predicate().equals(Predicate.IN)) {
          // IN works only on a LIST type
          v = handleValue(lhs.toString(), Column.Type.List.of(lhs.valueType()), bc.value());
        } else if (bc.predicate().equals(Predicate.CONTAINS)) {
          // for a CONTAINS, the valueType is a list. We need to extract the underlying value
          v = handleValue(lhs.toString(), lhs.valueType().fieldType(lhs.toString()), bc.value());
        } else if (bc.predicate().equals(Predicate.CONTAINS_KEY)) {
          // CONTAINS_KEY works only for Map, extract KEY type and use it
          ColumnType keyType = lhs.valueType().parameters().get(0);
          v = handleValue(lhs.toString(), keyType, bc.value());
        } else {
          v = handleValue(lhs.toString(), lhs.valueType(), bc.value());
        }

        if (!lhs.isUnset() && !v.isUnset()) {
          conditions.add(
              ImmutableCondition.builder().lhs(lhs).predicate(bc.predicate()).value(v).build());
        }
      }
    }

    private Condition.LHS createLHS(BuiltCondition.LHS lhs) {
      if (lhs.isColumnName()) {
        String name = lhs.columnName();
        return Condition.LHS.column(dml.table.existingColumn(name));
      } else if (lhs.isMapAccess()) {
        BuiltCondition.LHS.MapElement m = ((BuiltCondition.LHS.MapElement) lhs);
        Column column = dml.table.existingColumn(m.columnName());
        TypedValue v = handleValue(m.toString(), column.type().parameters().get(0), m.keyValue());
        return Condition.LHS.mapAccess(column, v);
      } else {
        throw new UnsupportedOperationException();
      }
    }

    List<TypedValue> internalBoundValues() {
      return Arrays.asList(internalBoundValues);
    }

    RowsImpacted rowsUpdated() {
      return rowsUpdated;
    }

    List<Modification> modifications() {
      return modifications;
    }

    List<Condition> conditions() {
      return conditions;
    }

    OptionalInt ttl() {
      return ttl == null ? OptionalInt.empty() : OptionalInt.of(ttl);
    }

    OptionalLong timestamp() {
      return timestamp == null ? OptionalLong.empty() : OptionalLong.of(timestamp);
    }
  }
}
