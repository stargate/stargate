package io.stargate.db.query.builder;

import static com.datastax.oss.driver.shaded.guava.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import io.stargate.db.query.PartitionKey;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.PrimaryKey;
import io.stargate.db.query.RowsImpacted;
import io.stargate.db.query.RowsImpacted.Ranges;
import io.stargate.db.query.RowsRange;
import io.stargate.db.query.RowsRange.Bound;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.javatuples.Pair;

/**
 * Processes built WHERE clauses when bind values are provided.
 *
 * <p>This currently only fully handles where clauses that results in a {@link RowsImpacted}, that
 * is WHERE clause that either selects a set of rows (all primary keys have conditions and they are
 * equals/IN) or select a partition slice (all partition keys have conditions and they are equals,
 * and clustering columns may or may not have conditions). Also, conditions on columns that are not
 * primary key column are ignored by this processor. Do note that this processor can be called on a
 * where clause that don't respect those conditions, but the {@link #process} method may return
 * {@code null}.
 */
abstract class WhereProcessor {
  private enum SelectionKind {
    KEYS,
    SINGLE_PARTITION_SLICE,
    PARTITION_RANGE
  }

  private final AbstractTable table;
  private final Codec valueCodec;
  private final List<Column> primaryKeys;
  private final PKCondition[] pkConditions;
  private boolean shouldIgnorePkConditions;

  WhereProcessor(AbstractTable table, Codec valueCodec) {
    this.table = table;
    this.valueCodec = valueCodec;
    this.primaryKeys = table.primaryKeyColumns();
    this.pkConditions = new PKCondition[primaryKeys.size()];
  }

  protected abstract TypedValue handleValue(String name, ColumnType type, Value<?> value);

  protected void onNonColumnNameLHS(BuiltCondition.LHS lhs) {}

  protected void onNonPrimaryKeyCondition(Column column) {}

  protected void onInequalityConditionOnPartitionKey(Column column, BuiltCondition condition) {}

  @Nullable
  RowsImpacted process(List<BuiltCondition> whereClause) {
    preProcess(whereClause);
    if (shouldIgnorePkConditions) {
      return null;
    }

    switch (kind()) {
      case KEYS:
        return createKeys();
      case SINGLE_PARTITION_SLICE:
        return createRange();
      default:
        return null;
    }
  }

  private SelectionKind kind() {
    for (int i = 0; i < primaryKeys.size(); i++) {
      Column column = primaryKeys.get(i);
      PKCondition pkCondition = pkConditions[i];
      if (pkCondition == null || !pkCondition.isEqOrIn()) {
        return column.isPartitionKey()
            ? SelectionKind.PARTITION_RANGE
            : SelectionKind.SINGLE_PARTITION_SLICE;
      }
    }
    return SelectionKind.KEYS;
  }

  private Ranges createRange() {
    Pair<List<List<TypedValue>>, Integer> p = populateEqAndIn(primaryKeys, pkConditions);
    int firstNonEq = p.getValue1();
    assert firstNonEq < primaryKeys.size();
    assert firstNonEq >= table.partitionKeyColumns().size()
        : firstNonEq + ": " + table.partitionKeyColumns();
    List<List<TypedValue>> pkValues = p.getValue0();

    PKCondition condition = pkConditions[firstNonEq];
    int partitionKeys = table.partitionKeyColumns().size();
    List<RowsRange> ranges = new ArrayList<>(pkValues.size());
    for (List<TypedValue> pkPrefix : pkValues) {
      PartitionKey partitionKey = new PartitionKey(table, pkPrefix.subList(0, partitionKeys));
      pkPrefix = pkPrefix.subList(partitionKeys, pkPrefix.size());
      List<TypedValue> startValues = pkPrefix;
      boolean startInclusive = true;
      List<TypedValue> endValues = pkPrefix;
      boolean endInclusive = true;
      if (condition != null) {
        startValues = new ArrayList<>(pkPrefix);
        startValues.add(condition.values[0]);
        startInclusive = condition.isInclusive[0];
        endValues = new ArrayList<>(pkPrefix);
        endValues.add(condition.values[1]);
        endInclusive = condition.isInclusive[1];
      }
      RowsRange.Bound start = new Bound(startValues, startInclusive);
      RowsRange.Bound end = new Bound(endValues, endInclusive);
      ranges.add(new RowsRange(partitionKey, start, end));
    }
    return new Ranges(ranges);
  }

  private RowsImpacted.Keys createKeys() {
    Pair<List<List<TypedValue>>, Integer> p = populateEqAndIn(primaryKeys, pkConditions);
    // All keys must have been consumed, or we had a unexpected condition
    checkArgument(
        p.getValue1() == primaryKeys.size(),
        "Invalid condition combinations on primary key columns");
    List<List<TypedValue>> pkValues = p.getValue0();
    List<PrimaryKey> keys = new ArrayList<>(pkValues.size());
    for (List<TypedValue> pkValue : pkValues) {
      keys.add(new PrimaryKey(table, pkValue));
    }
    return new RowsImpacted.Keys(keys);
  }

  private Pair<List<List<TypedValue>>, Integer> populateEqAndIn(
      List<Column> primaryKeys, PKCondition[] pkConditions) {
    List<List<TypedValue>> pkValues = new ArrayList<>();
    pkValues.add(new ArrayList<>());
    for (int i = 0; i < primaryKeys.size(); i++) {
      Column column = primaryKeys.get(i);
      PKCondition condition = pkConditions[i];
      if (condition == null || !condition.isEqOrIn()) {
        return Pair.with(pkValues, i);
      }

      TypedValue value = condition.values[0];
      if (condition.isEq()) {
        // Adds the new value to all keys.
        for (List<TypedValue> pk : pkValues) {
          pk.add(value);
        }
      } else { // It's a IN
        assert value.javaValue() instanceof List; // Things would have failed before otherwise
        List<?> inValues = (List<?>) value.javaValue();
        List<TypedValue> inTypedValues =
            inValues.stream()
                .map(o -> TypedValue.forJavaValue(valueCodec, column.name(), column.type(), o))
                .collect(Collectors.toList());
        // For each existing primary keys, creates #inValues new keys corresponding to the
        // previous key plus the new value.
        // TODO: note that if we're not careful with the generated queries, we can have a
        //  combinatorial explosion here. I could swear C* had limits for this, rejecting queries
        //  that would create too many keys, but I can't find it right now, so maybe not. We
        //  may want to add in any case, but it's unclear what a good number is.
        List<List<TypedValue>> currentValues = pkValues;
        pkValues = new ArrayList<>();
        for (List<TypedValue> currentValue : currentValues) {
          for (TypedValue newValue : inTypedValues) {
            List<TypedValue> newValues = new ArrayList<>(currentValue);
            newValues.add(newValue);
            pkValues.add(newValues);
          }
        }
      }
    }
    return Pair.with(pkValues, primaryKeys.size());
  }

  void preProcess(List<BuiltCondition> whereClause) {
    for (BuiltCondition condition : whereClause) {
      BuiltCondition.LHS lhs = condition.lhs();
      Column column = table.existingColumn(lhs.columnName());
      if (!column.isPrimaryKeyComponent()) {
        onNonPrimaryKeyCondition(column);
        continue;
      }

      if (!lhs.isColumnName()) {
        onNonColumnNameLHS(lhs);
        this.shouldIgnorePkConditions = true;
        return;
      }

      int idx = table.primaryKeyColumnIndex(column);
      PKCondition pkCondition = compute(column, pkConditions[idx], condition);
      if (pkCondition == PKCondition.INVALID) {
        this.shouldIgnorePkConditions = true;
        return;
      }
      pkConditions[idx] = pkCondition;
    }
  }

  private static boolean isEqOrIn(BuiltCondition condition) {
    return condition.predicate() == Predicate.EQ || condition.predicate() == Predicate.IN;
  }

  private static int rangeIdx(BuiltCondition condition) {
    switch (condition.predicate()) {
      case GT:
      case GTE:
        return 0;
      case LT:
      case LTE:
        return 1;
      default:
        throw new IllegalArgumentException(
            "Invalidate condition on primary key column: " + condition);
    }
  }

  private static boolean isInclusive(BuiltCondition condition) {
    switch (condition.predicate()) {
      case GTE:
      case LTE:
        return true;
      case GT:
      case LT:
        return false;
      default:
        // This should be called after rangeIdx, which already rejected those.
        throw new AssertionError();
    }
  }

  private PKCondition compute(Column column, PKCondition existing, BuiltCondition condition) {
    TypedValue v;
    if (condition.predicate() == Predicate.IN) {
      v =
          handleValue(
              format("in(%s)", column.name()), Type.List.of(column.type()), condition.value());
    } else {
      v = handleValue(column.name(), column.type(), condition.value());
    }
    checkArgument(
        v.bytes() != null,
        "Cannot use a null value for primary key column %s in table %s",
        column.cqlName(),
        table.cqlQualifiedName());

    if (v.isUnset()) {
      return existing;
    }

    if (isEqOrIn(condition)) {
      if (existing != null) {
        throw new IllegalArgumentException(
            format("Incompatible conditions %s and %s", existing.firstCondition, condition));
      }

      PKCondition pkCondition = new PKCondition(condition);
      pkCondition.values[0] = v;
      return pkCondition;
    }

    if (column.isPartitionKey()) {
      onInequalityConditionOnPartitionKey(column, condition);
      return PKCondition.INVALID;
    }

    if (existing == null) {
      existing = new PKCondition(condition);
    } else {
      checkArgument(
          !existing.isEqOrIn(),
          "Incompatible conditions %s and %s",
          existing.firstCondition,
          condition);
    }
    int idx = rangeIdx(condition);
    checkArgument(
        existing.values[idx] == null,
        "Incompatible conditions %s and %s",
        existing.firstCondition,
        condition);
    existing.values[idx] = v;
    existing.isInclusive[idx] = isInclusive(condition);
    return existing;
  }

  private static class PKCondition {
    private static final PKCondition INVALID = new PKCondition(null);

    private final BuiltCondition firstCondition;
    // for Eq/IN, only 0 is set, for ranges, 0 = open, 1 = close
    private final TypedValue[] values = new TypedValue[2];
    // Only set for ranges, 0 = open, 1 = close
    private final boolean[] isInclusive = new boolean[2];

    private PKCondition(BuiltCondition firstCondition) {
      this.firstCondition = firstCondition;
    }

    private boolean isEq() {
      return firstCondition.predicate() == Predicate.EQ;
    }

    private boolean isEqOrIn() {
      return WhereProcessor.isEqOrIn(firstCondition);
    }
  }
}
