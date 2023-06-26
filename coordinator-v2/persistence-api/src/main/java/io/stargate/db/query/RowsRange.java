package io.stargate.db.query;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A range of primary keys within a given partition (so a range of clustering columns for a given
 * partition key).
 */
public final class RowsRange {
  private final PartitionKey partitionKey;
  private final Bound clusteringStart;
  private final Bound clusteringEnd;

  public RowsRange(PartitionKey partitionKey, Bound clusteringStart, Bound clusteringEnd) {
    this.partitionKey = partitionKey;
    this.clusteringStart = clusteringStart;
    this.clusteringEnd = clusteringEnd;
  }

  public PartitionKey partitionKey() {
    return partitionKey;
  }

  public Bound clusteringStart() {
    return clusteringStart;
  }

  public Bound clusteringEnd() {
    return clusteringEnd;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RowsRange)) {
      return false;
    }
    RowsRange rowsRange = (RowsRange) o;
    return partitionKey.equals(rowsRange.partitionKey)
        && clusteringStart.equals(rowsRange.clusteringStart)
        && clusteringEnd.equals(rowsRange.clusteringEnd);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionKey, clusteringStart, clusteringEnd);
  }

  public static final class Bound {
    public static Bound EMPTY = new Bound(Collections.emptyList(), true);

    private final List<TypedValue> values;
    private final boolean isInclusive;

    public Bound(List<TypedValue> values, boolean isInclusive) {
      this.values = values;
      this.isInclusive = isInclusive;
    }

    public boolean isEmpty() {
      return values.isEmpty();
    }

    public List<TypedValue> values() {
      return values;
    }

    public boolean isInclusive() {
      return isInclusive;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Bound)) {
        return false;
      }
      Bound bound = (Bound) o;
      return isInclusive == bound.isInclusive && values.equals(bound.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(values, isInclusive);
    }
  }
}
